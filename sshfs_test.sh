#!/bin/bash

# sudo dnf install sshfs

sshfs --version

if [ ! -d "/tmp/my_remote_mount" ]; then
    mkdir /tmp/my_remote_mount
fi

sshfs rocky@localhost:/home/rocky /tmp/my_remote_mount/

ls /tmp/my_remote_mount

echo 'I am with you alway, even unto the end of the world.' | tee -a /tmp/my_remote_mount/test.txt

more /tmp/my_remote_mount/test.txt

ls /home/rocky

more /home/rocky/test.txt

rm -f /tmp/my_remote_mount/test.txt

fusermount -u /tmp/my_remote_mount

rmdir /tmp/my_remote_mount