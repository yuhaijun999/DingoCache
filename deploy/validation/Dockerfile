# VM wrapper for hosts whose shared Docker kernel cannot run io_uring.
# /dev/kvm is optional at runtime; no host modules or host networking required.
ARG BASE_IMAGE=docker.io/library/ubuntu@sha256:9abcaffc92a5279fff3a6a41635c0800ea33e528425fb389658806476e7defde
FROM ${BASE_IMAGE}
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates cloud-image-utils curl openssh-client python3 \
    qemu-system-x86 qemu-utils && rm -rf /var/lib/apt/lists/*
WORKDIR /work
CMD ["sleep", "infinity"]
