#!/bin/bash

# 1. Install Ansible
# sudo apt install ansible -y

ansible localhost -m debug -a "msg='Hello Frontier World\!'"

ansible-playbook ansible_test.yaml
