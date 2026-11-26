"""Pure mixed-prefix controller tests; never start a native service."""
from contextlib import ExitStack
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'deploy/validation'))
import run_prefix_mixed as runner
from test_long_residence_runner import metrics


def fixture(policy, dirs, route, initial=False):
    text=metrics(policy, dirs, initial)
    for index,path in enumerate(dirs):
        label='{disk='+json.dumps(str(path))+',disk_index="'+str(index)+'"}'
        text += f'dfkv_slab_disk_capacity_bytes{label} 4294967296\n'
        if route=='watermark':
            for suffix,value in [('watermark_high_bytes',2147483600),('watermark_low_bytes',1717986880),('reclaim_interval_ms',50)]:
                text=text.replace(f'dfkv_slab_disk_{suffix}{label} 0',f'dfkv_slab_disk_{suffix}{label} {value}')
        text+=f'dfkv_slab_disk_watermark_high_observations_total{label} {0 if initial or route!="watermark" else 2}\n'
        text+=f'dfkv_slab_disk_watermark_last_high_bytes{label} {0 if initial or route!="watermark" else 2200000000}\n'
    for name,value in [('dfkv_slab_watermark_freed_slot_bytes_total',4194304),('dfkv_slab_watermark_freed_payload_bytes_total',8192)]:
        text+=f'{name} {0 if initial or route!="watermark" else value}\n'
    for slot,value in [(1048576,60),(2097152,40)]:
        text+=f'dfkv_slab_class_put_total{{slot_size="{slot}"}} {0 if initial else value}\n'
    if route=='watermark' and not initial:
        text=text.replace('dfkv_slab_watermark_evictions_total 0','dfkv_slab_watermark_evictions_total 4')
        for suffix in ('attempts','completed'):
            text=text.replace(f'dfkv_slab_metadata_ops_{suffix}_total{{path="watermark_evict"}} 0',f'dfkv_slab_metadata_ops_{suffix}_total{{path="watermark_evict"}} 4')
    return text


class PrefixMixedRunnerTest(unittest.TestCase):
    def setUp(self):
        self.dirs=['/data/disk1/cases/a','/data/disk2/cases/a','/data/disk3/cases/a']
        self.summary=dict(native=dict(put_acks=100,put_bytes=409600,put_attempts=100,put_errors=0,put_unknown=0,
            get_hits=17,get_misses=3,get_attempts=20,get_errors=0,get_unknown=0))
        self.classes={1048576:60,2097152:40}

    def test_three_pairs_and_split_identity(self):
        rows=runner.plan()
        self.assertEqual([r['policy'] for r in rows],['legacy','lifetime','lifetime','legacy','legacy','lifetime'])
        self.assertEqual(runner.plan(2,1),rows[2:])
        for args in [(True,0),(0,0),(4,0),(2,2),(1,-1)]:
            with self.subTest(args=args),self.assertRaises(ValueError):runner.plan(*args)

    def test_environments_clear_tuning_and_route_only_changes(self):
        with mock.patch.dict(runner.os.environ,{'DFKV_SLAB_PROTECT_SEC':'1','DFKV_RDMA':'0'},clear=True):
            a,b=runner.environment('class-pressure'),runner.environment('watermark')
        self.assertNotIn('DFKV_SLAB_PROTECT_SEC',b)
        self.assertEqual(b['DFKV_RDMA'],'1')
        self.assertEqual({k for k in a if a[k]!=b[k]}, {'DFKV_SLAB_RECLAIM_MS','DFKV_SLAB_EVICT_LOW_PCT','DFKV_SLAB_EVICT_HIGH_PCT'})
        with self.assertRaises(ValueError):runner.environment('bad')

    def test_route_metrics_and_donor_qualification(self):
        for route in ('class-pressure','watermark'):
            for policy in ('legacy','lifetime'):
                before,after=fixture(policy,self.dirs,route,True),fixture(policy,self.dirs,route)
                result=runner.check_metrics(before,after,policy,self.summary,self.dirs,route,self.classes)
                self.assertEqual(result['capacity_evictions'],64)
                self.assertEqual(result['watermark_evictions'],4 if route=='watermark' else 0)
                after=after.replace('dfkv_slab_extent_steals_total 0','dfkv_slab_extent_steals_total 3')
                result=runner.check_metrics(before,after,policy,self.summary,self.dirs,route,self.classes)
                self.assertTrue(result['donor_confounded'])
                self.assertEqual(result['donor_extent_steals'],3)

    def test_reject_false_watermark_progress_and_wrong_effective_thresholds(self):
        before,after=fixture('legacy',self.dirs,'watermark',True),fixture('legacy',self.dirs,'watermark')
        for old,new in [('2147483600','0'),(' 2\n',' 0\n'),(' 2200000000',' 0'),('dfkv_slab_watermark_evictions_total 4','dfkv_slab_watermark_evictions_total 0'),('dfkv_slab_watermark_freed_payload_bytes_total 8192','dfkv_slab_watermark_freed_payload_bytes_total 0')]:
            with self.subTest(old=old),self.assertRaises((ValueError,RuntimeError)):
                runner.check_metrics(before,after.replace(old,new),'legacy',self.summary,self.dirs,'watermark',self.classes)

    def test_reject_class_only_competing_route_and_bad_native_denominators(self):
        before,after=fixture('legacy',self.dirs,'class-pressure',True),fixture('legacy',self.dirs,'class-pressure')
        for key,value in [('get_unknown',1),('put_errors',1),('get_attempts',19),('put_bytes',-1),('get_hits',True)]:
            summary=dict(native=dict(self.summary['native']));summary['native'][key]=value
            with self.subTest(key=key),self.assertRaises(ValueError):
                runner.check_metrics(before,after,'legacy',summary,self.dirs,'class-pressure',self.classes)
        for old,new in [('dfkv_slab_watermark_evictions_total 0','dfkv_slab_watermark_evictions_total 1'),('dfkv_slab_reclaimed_total 0','dfkv_slab_reclaimed_total 1'),('dfkv_slab_class_put_total{slot_size="1048576"} 60','dfkv_slab_class_put_total{slot_size="1048576"} 59'),('dfkv_uring_init_fallbacks_total 0','dfkv_uring_init_fallbacks_total 1'),('dfkv_slab_healthy 1','dfkv_slab_healthy 0')]:
            with self.subTest(new=new),self.assertRaises((ValueError,RuntimeError)):
                runner.check_metrics(before,after.replace(old,new),'legacy',self.summary,self.dirs,'class-pressure',self.classes)

    def test_duplicate_disk_labels_and_class_labels_rejected(self):
        before,after=fixture('legacy',self.dirs,'watermark',True),fixture('legacy',self.dirs,'watermark')
        for line in ['dfkv_slab_class_put_total{slot_size="1048576"} 0\n',f'dfkv_disk_objects{{disk={json.dumps(self.dirs[0])}}} 0\n']:
            with self.subTest(line=line),self.assertRaises((ValueError,RuntimeError)):
                runner.check_metrics(before,after+line,'legacy',self.summary,self.dirs,'watermark',self.classes)

    def test_put_classes_sum_objects_not_calls(self):
        plan={'config':{}}
        workload=SimpleNamespace(object_for=lambda plan,index:{'length':4096 if index%2==0 else 1048577})
        report={'events':[{'op':'put','status':'ack','ids':[0,1,2]},{'op':'get','status':'hit','ids':[0]}, {'op':'put','status':'ack','ids':[1]}]}
        self.assertEqual(runner.put_classes(workload,plan,report),{1048576:2,2097152:2})

    def test_invalid_arguments_before_guest_or_spawn(self):
        with mock.patch.object(runner.common,'guest_paths') as paths,mock.patch.object(runner.subprocess,'Popen') as spawn:
            for kwargs in [{'route':'bad'},{'route':'watermark','pairs':4},{'route':'watermark','steps':0}]:
                with self.subTest(kwargs=kwargs),self.assertRaises(ValueError):runner.run('/missing',[],'/unused','tag',**kwargs)
            with self.assertRaises(ValueError):runner.run('/missing',['/data/a,b'],'/unused','tag')
            paths.assert_not_called();spawn.assert_not_called()

    def test_sampler_retains_prom_resources_and_hash(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp); proc=root/'proc';proc.mkdir()
            (proc/'stat').write_text('123 (server) S '+'0 '*10+'17 18 '+'0 '*6+'4242 '+'0 '*5+'\n')
            (proc/'status').write_text('Name:\tserver\nVmRSS:\t123 kB\nThreads:\t4\n')
            with mock.patch.object(runner.baseline,'_scrape',return_value='dfkv_storage_healthy 1\ndfkv_slab_healthy 1\ndfkv_errors_total 0\ndfkv_uring_init_fallbacks_total 0\n'):
                sampler=runner.Sampler(root,123,proc=proc)
                sampler.sample();sampler.sample()
            rows=json.loads((root/'metrics-samples.json').read_text())
            self.assertEqual(len(rows),2)
            self.assertEqual(rows[0]['resource']['rss_kib'],123)
            self.assertEqual(rows[0]['resource']['threads'],4)
            self.assertEqual(rows[0]['sha256'],runner.baseline.file_sha(root/rows[0]['file']))

    def test_client_timeout_and_sampling_failure_reap_owned_child(self):
        for error in (TimeoutError('deadline'),RuntimeError('sampler failed')):
            with self.subTest(error=error), tempfile.TemporaryDirectory() as tmp:
                child=mock.Mock();child.poll.return_value=None
                child.wait.return_value=-15
                sampler=mock.Mock()
                if isinstance(error,RuntimeError):sampler.sample.side_effect=error
                with mock.patch.object(runner.subprocess,'Popen',return_value=child),mock.patch.object(runner.time,'monotonic',side_effect=[0,2200] if isinstance(error,TimeoutError) else [0,0]):
                    with self.assertRaises(type(error)):
                        runner.run_client(['unused'],{},Path(tmp)/'client.log',sampler,timeout=2100)
                child.terminate.assert_called_once();child.wait.assert_called()

    def test_startup_checks_default_scores_and_selected_route(self):
        scores={'DFKV_SLAB_LIFE_BASE_SEC':'604800','DFKV_SLAB_LIFE_READ_BONUS_SEC':'302400',
                'DFKV_SLAB_LIFE_MAX_READS':'2','DFKV_SLAB_PROTECT_SEC':'180',
                'DFKV_SLAB_PROTECT_SLOWDOWN':'4','DFKV_SLAB_PROTECT_BOOST_SEC':'1209600',
                'DFKV_SLAB_KEEP_SCORE_SEC':'475200','DFKV_SLAB_BUCKET_SEC':'43200','DFKV_SLAB_BUCKET_COUNT':'24'}
        for route in runner.ROUTES:
            for policy in ('legacy','lifetime'):
                env=runner.environment(route)
                values=dict(DFKV_PUT_ACK_MODE='disk',DFKV_RAM_TIER='off',DFKV_SERVER_URING='on',
                            disk_eviction_requested=policy,disk_eviction_effective=policy)
                values.update({key:env[key] for key in ('DFKV_SLAB_RECLAIM_MS','DFKV_SLAB_EVICT_LOW_PCT','DFKV_SLAB_EVICT_HIGH_PCT')})
                values.update({key:value if policy=='lifetime' else 'inactive' for key,value in scores.items()})
                text='\n'.join(key+' = '+value+' (fixture)' for key,value in values.items())
                runner.check_startup(text,policy,route)
                for key,value in values.items():
                    with self.subTest(route=route,policy=policy,key=key),self.assertRaises(ValueError):
                        runner.check_startup(text.replace(key+' = '+value,key+' = WRONG'),policy,route)

    def test_metadata_snapshot_not_fabricated_atomic_and_errors_still_fail(self):
        before,after=fixture('legacy',self.dirs,'watermark',True),fixture('legacy',self.dirs,'watermark')
        after=after.replace('dfkv_slab_metadata_ops_attempts_total{path="watermark_evict"} 4',
                            'dfkv_slab_metadata_ops_attempts_total{path="watermark_evict"} 5')
        result=runner.check_metrics(before,after,'legacy',self.summary,self.dirs,'watermark',self.classes)
        self.assertEqual(result['watermark_evict_metadata_attempts'],5)
        with self.assertRaises(ValueError):
            runner.check_metrics(before,after.replace('dfkv_slab_metadata_ops_errors_total{path="watermark_evict"} 0',
                'dfkv_slab_metadata_ops_errors_total{path="watermark_evict"} 1'),'legacy',self.summary,self.dirs,'watermark',self.classes)

    def _matrix(self, *, failure=None):
        stack=ExitStack();self.addCleanup(stack.close)
        root=Path(stack.enter_context(tempfile.TemporaryDirectory()))
        build=root/'build';build.mkdir()
        for name in ('dfkv_server','libdfkv.so'):(build/name).write_bytes(b'fixture-no-execution')
        roots=[root/('disk'+str(i)) for i in (1,2,3)]
        for path in roots:path.mkdir()
        output=root/'out'
        workload=SimpleNamespace(generate=mock.Mock(return_value={'sha256':'plan'}),validate=mock.Mock(),
            load=lambda path:json.loads(Path(path).read_text()),check=mock.Mock(return_value=self.summary))
        stack.enter_context(mock.patch.dict(sys.modules,{'prefix_delayed':workload}))
        stack.enter_context(mock.patch.object(runner.common,'guest_paths',return_value=(roots,output)))
        stack.enter_context(mock.patch.object(runner.quick,'check_ports'))
        stack.enter_context(mock.patch.object(runner,'check_initial'))
        startup=stack.enter_context(mock.patch.object(runner,'check_startup'))
        oracle=stack.enter_context(mock.patch.object(runner,'check_metrics',return_value={'capacity_evictions':4}))
        stack.enter_context(mock.patch.object(runner,'put_classes',return_value=self.classes))
        stack.enter_context(mock.patch.object(runner.baseline,'_ready',return_value='before'))
        stack.enter_context(mock.patch.object(runner.baseline,'_scrape',return_value='after'))
        tools=stack.enter_context(mock.patch.object(runner,'_tools',return_value={'fixture':'sha'}))
        sampler=stack.enter_context(mock.patch.object(runner,'Sampler'))
        sampler.return_value.rows=[{'fixture':'sample'}]
        processes=[]
        def spawn(command,**kwargs):
            child=mock.Mock(pid=len(processes)+100)
            child.returncode=None
            child.poll.side_effect=lambda:child.returncode
            def wait(timeout):
                child.returncode=0
                return 0
            child.wait.side_effect=wait
            processes.append(child)
            return child
        stack.enter_context(mock.patch.object(runner.subprocess,'Popen',side_effect=spawn))
        def client(command,env,log_path,sampler,*,receipt):
            receipt.update(pid=200,exit=0,reaped=True,state='exited')
            path=Path(command[command.index('--output')+1]);path.mkdir()
            (path/'report.json').write_text(json.dumps({'passed':True,'transport':'rdma'}))
            if failure=='timeout':raise TimeoutError('fixture timeout')
            if failure=='build':(build/'libdfkv.so').write_bytes(b'mutated')
            if failure=='tools':tools.return_value={'fixture':'changed'}
            if failure=='plan':(output/'plan.json').write_text('{}')
            if failure=='transport':(path/'report.json').write_text(json.dumps({'passed':True,'transport':'fake-rdma-test'}))
        clients=stack.enter_context(mock.patch.object(runner,'run_client',side_effect=client))
        if failure=='oracle':oracle.side_effect=ValueError('fixture failed native oracle')
        if failure=='cleanup':
            original=runner.common.stop_owned
            def stop(child):
                original(child)
                raise RuntimeError('fixture cleanup failure')
            stack.enter_context(mock.patch.object(runner.common,'stop_owned',side_effect=stop))
        result=runner.run(build,roots,output,'tag',route='watermark',steps=12,delays_sec=[1,3,7])
        return result,output,processes,clients,startup,workload

    def test_matrix_fixed_three_pairs_native_contract_and_reap(self):
        result,output,processes,clients,startup,workload=self._matrix()
        self.assertTrue(result['passed']);self.assertEqual(clients.call_count,6)
        self.assertEqual(startup.call_count,6)
        workload.generate.assert_called_once_with(sizes=[4096,1048577],steps=12,arrival_batch=64,due_first=True,delays_sec=[1,3,7])
        self.assertEqual(len({path for row in result['cases'] for path in row['data_dirs']}),18)
        for row,child in zip(result['cases'],processes):
            self.assertEqual(row['state'],'passed')
            self.assertEqual(row['environment']['DFKV_RDMA'],'1')
            self.assertEqual(row['client_timeout_sec'],2100)
            self.assertTrue(row['server_reaped']);self.assertEqual(row['server_exit'],0)
            child.terminate.assert_called_once()
        self.assertEqual(json.loads((output/'matrix.json').read_text()),result)

    def test_matrix_failure_preserved_later_cases_not_attempted_and_server_reaped(self):
        for failure in ('timeout','build','tools','plan','transport','oracle','cleanup'):
            with self.subTest(failure=failure):
                result,output,processes,clients,startup,workload=self._matrix(failure=failure)
                self.assertFalse(result['passed']);self.assertEqual(clients.call_count,1)
                self.assertEqual(result['cases'][0]['state'],'failed')
                self.assertTrue(result['cases'][0]['server_reaped'])
                self.assertTrue(all(row['state']=='not_attempted' for row in result['cases'][1:]))
                self.assertTrue((output/'case-00-legacy/client/report.json').is_file())
                self.assertTrue((output/'matrix.json').is_file())

    def test_client_success_final_sample_and_failure_exit_preserved(self):
        for returncode in (0,7):
            with self.subTest(returncode=returncode),tempfile.TemporaryDirectory() as tmp:
                child=mock.Mock();child.poll.return_value=returncode;child.wait.return_value=returncode
                sampler=mock.Mock()
                with mock.patch.object(runner.subprocess,'Popen',return_value=child):
                    if returncode:
                        with self.assertRaises(runner.subprocess.CalledProcessError):
                            runner.run_client(['unused'],{},Path(tmp)/'client.log',sampler)
                        sampler.sample.assert_not_called()
                    else:
                        runner.run_client(['unused'],{},Path(tmp)/'client.log',sampler)
                        sampler.sample.assert_called_once()
                child.terminate.assert_not_called();child.wait.assert_called_once()

    def test_client_deadline_escalates_only_owned_child_and_reaps(self):
        with tempfile.TemporaryDirectory() as tmp:
            child=mock.Mock();child.poll.return_value=None
            child.poll.side_effect=[None,None,None,-9]
            child.wait.side_effect=[runner.subprocess.TimeoutExpired('owned',5),-9]
            with mock.patch.object(runner.subprocess,'Popen',return_value=child),\
                    mock.patch.object(runner.time,'monotonic',side_effect=[0,2200]):
                with self.assertRaises(TimeoutError):
                    runner.run_client(['unused'],{},Path(tmp)/'client.log',mock.Mock())
            child.terminate.assert_called_once();child.kill.assert_called_once()
            self.assertEqual(child.wait.call_args_list,[mock.call(timeout=5),mock.call(timeout=3)])

    def test_fractional_pressure_counter_and_all_metadata_errors_rejected(self):
        before,after=fixture('legacy',self.dirs,'watermark',True),fixture('legacy',self.dirs,'watermark')
        for old,new in [('dfkv_slab_extent_steals_total 0','dfkv_slab_extent_steals_total 0.5'),
                        ('dfkv_slab_disk_watermark_high_observations_total{disk="/data/disk1/cases/a",disk_index="0"} 2',
                         'dfkv_slab_disk_watermark_high_observations_total{disk="/data/disk1/cases/a",disk_index="0"} 2.5')]:
            with self.subTest(new=new),self.assertRaises(ValueError):
                runner.check_metrics(before,after.replace(old,new),'legacy',self.summary,self.dirs,'watermark',self.classes)
        with self.assertRaises(ValueError):
            runner.check_metrics(before,after+'dfkv_slab_metadata_ops_errors_total{path="donor_bind"} 1\n',
                                 'legacy',self.summary,self.dirs,'watermark',self.classes)

    def test_client_unreaped_and_spawn_failure_receipts_are_truthful(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            child=mock.Mock(pid=999);child.poll.return_value=None
            child.wait.side_effect=runner.subprocess.TimeoutExpired('owned',3)
            receipt={}
            with mock.patch.object(runner.subprocess,'Popen',return_value=child),\
                    mock.patch.object(runner.time,'monotonic',side_effect=[0,2200]):
                with self.assertRaises(runner.subprocess.TimeoutExpired):
                    runner.run_client(['unused'],{},root/'hung.log',mock.Mock(),receipt=receipt)
            self.assertEqual(receipt['state'],'cleanup_failed')
            self.assertFalse(receipt['reaped']);self.assertIsNone(receipt['exit'])
            self.assertEqual(receipt['pid'],999)
            self.assertIn('deadline',receipt['error'])
            self.assertIn('TimeoutExpired',receipt['cleanup_error'])
            receipt={}
            with mock.patch.object(runner.subprocess,'Popen',side_effect=OSError('no process')):
                with self.assertRaises(OSError):
                    runner.run_client(['unused'],{},root/'spawn.log',mock.Mock(),receipt=receipt)
            self.assertEqual(receipt['state'],'spawn_failed')
            self.assertIsNone(receipt['pid']);self.assertFalse(receipt['reaped'])

    def test_class_admission_real_counterexample_accepts_ack_upserts_without_new_slot(self):
        # Actual lifetime preflight class observations: 21 acknowledged refill
        # upserts did not all allocate a new slot. GETs use the generic fixture;
        # ACKs, payload bytes and per-class observations below are native data.
        before=fixture('legacy',self.dirs,'class-pressure',True)
        after=fixture('legacy',self.dirs,'class-pressure')
        after=after.replace('dfkv_cache_put_total 100','dfkv_cache_put_total 13093')
        after=after.replace('dfkv_bytes_written_total 409600','dfkv_bytes_written_total 6891846035')
        after=after.replace('slot_size="1048576"} 60','slot_size="1048576"} 6535')
        after=after.replace('slot_size="2097152"} 40','slot_size="2097152"} 6537')
        summary=dict(native=dict(self.summary['native'],put_acks=13093,put_attempts=13093,put_bytes=6891846035))
        result=runner.check_metrics(before,after,'legacy',summary,self.dirs,'class-pressure',
                                    {1048576:6546,2097152:6547},distinct_classes={1048576:6528,2097152:6528})
        self.assertEqual(result['per_class_ack_upserts'],{'1048576':6546,'2097152':6547})
        self.assertEqual(result['per_class_distinct_objects'],{'1048576':6528,'2097152':6528})
        self.assertEqual(result['per_class_allocator_admissions'],{'1048576':6535,'2097152':6537})

    def test_distinct_ack_identity_counts_exclude_repeated_refills(self):
        workload=SimpleNamespace(object_for=lambda plan,index:{'length':4096 if index%2==0 else 1048577})
        report={'events':[{'op':'put','status':'ack','ids':[0,1,2]},
                          {'op':'put','status':'ack','ids':[1,2]},
                          {'op':'put','status':'unknown','ids':[3]}]}
        self.assertEqual(runner.put_classes(workload,{},report),{1048576:3,2097152:2})
        self.assertEqual(runner.put_classes(workload,{},report,distinct=True),{1048576:2,2097152:1})

    def test_admission_bounds_reject_missing_zero_too_low_too_high_and_unused_classes(self):
        before=fixture('legacy',self.dirs,'class-pressure',True)
        after=fixture('legacy',self.dirs,'class-pressure')
        distinct={1048576:50,2097152:30}
        for old,new in [('slot_size="1048576"} 60','slot_size="1048576"} 0'),
                        ('slot_size="1048576"} 60','slot_size="1048576"} 49'),
                        ('slot_size="1048576"} 60','slot_size="1048576"} 61'),
                        ('slot_size="2097152"} 40','slot_size="4194304"} 40')]:
            with self.subTest(new=new),self.assertRaises(ValueError):
                runner.check_metrics(before,after.replace(old,new),'legacy',self.summary,self.dirs,
                                     'class-pressure',self.classes,distinct_classes=distinct)
        for counts in ({1048576:0,2097152:30},{1048576:61,2097152:30},{1048576:50},
                       {1048576:True,2097152:30}):
            with self.subTest(counts=counts),self.assertRaises(ValueError):
                runner.check_metrics(before,after,'legacy',self.summary,self.dirs,'class-pressure',
                                     self.classes,distinct_classes=counts)
        with self.assertRaises(ValueError):
            runner.check_metrics(before+'dfkv_slab_class_put_total{slot_size="4194304"} 0\n',
                after+'dfkv_slab_class_put_total{slot_size="4194304"} 1\n','legacy',self.summary,
                self.dirs,'class-pressure',self.classes,distinct_classes=distinct)


if __name__=='__main__':unittest.main()
