#!/usr/bin/env bash


sudo mkdir -p /mnt/disks/work \
  && sudo mkfs.ext4 -F /dev/nvme0n1 \
  && sudo mount /dev/nvme0n1 /mnt/disks/work/ \
  && cd /mnt/disks/work

sudo mkdir -p /mnt/disks/back \
  && sudo mount /dev/sdb1 /mnt/disks/back

sudo apt-get update -y \
  && sudo apt-get upgrade -yq

sudo apt-get install -yq htop docker.io docker-compose \
  && sudo usermod -aG docker $USER
