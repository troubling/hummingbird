Hansible
========

TL;DR
-----

1.  Install the latest version of [Ansible](http://docs.ansible.com/ansible/latest/intro_installation.html)
1.  Edit the hosts file to have the hosts you want and modify variables as needed.
1.  Run the storage playbook `ansible-playbook -i hosts storage.yml` to partition and format the devices.
1.  Run the hummingbird playbook `ansible-playbook -i hosts hummingbird.yml` to install and configure the latest hummingbird.
1.  Create rings for the cluster.
1.  Run the ring playbook `ansible-playbook -i hosts ring.yml` to copy the rings to each server.

Inventory
---------

Each storage node should be listed in the `hummingbird` group.  The 'service_ip' variable is the ip that the non-public services will listen on.  

### hummingbird group variables

Variable | Description
-------- | -----------
`obj_devs` | A list of devices used as storage for the object server.  This can also be overriden at the server level.
`meta_devs` | A list of devices used as storage for the container and account servers.  This can be overriden at the server level.
`proxy_port` | Port the proxy server will listen on.
`container_port` | Port the container server will listen on.
`account_port` | Port the account server will listen on.
`object_replicator_port` | Port the object replicator will listen on.
`auth_uri` | Auth URI for keystone
`auth_url` | Auth URL for keystone
`authtoken_username` | Username to use when validating auth with keystone.
`authtoken_password` | Password to use when validating auth with keystone.

Playbooks
---------

### storage.yml

This playbook will format and partition the devices in the `hummingbird` group.  If devices already have a mounted filesystem, it will do its best to unmount and reset, but may require manual intervention if there are files open or something else keeping the filesystem busy.

**WARNING:**  Only run this if you want to erase the drives, this will re-format devices even if they already have a filesystem!

### hummingbird.yml

This playbook will install and configure the latest version of hummingbird to all of the nodes in the `hummingbird` group.

### ring.yml

This playbook will copy the account container and object rings from the local `/etc/hummingbird` directory to all nodes in the `hummingbird` group.

Secrets
-------
```
ansible-vault encrypt_string <string_to_encrypt> --ask-vault-pass
```

Will prompt you for a password. It results in:

```
!vault |
          $ANSIBLE_VAULT;1.1;AES256
          31633734306439336165653439346662323730393765633466353634333133316566373961663739
          6539633735306336373465306336373366653761656666620a653464373461363234346339333431
          37376337396362346266613061383862323636316432353230633136656161313661643939363866
          3838346232316434360a633964643434633763336565386339643764616566383661373663333062
          34316463363061633363623339383864626162323661396262303562656433343233
```

You can paste this into a yaml inventory file. (Not an INI file.)

```
hummingbird:
  hosts:
    localhost:
  vars:
    hash_prefix: !vault |
          $ANSIBLE_VAULT;1.1;AES256
          31633734306439336165653439346662323730393765633466353634333133316566373961663739
          6539633735306336373465306336373366653761656666620a653464373461363234346339333431
          37376337396362346266613061383862323636316432353230633136656161313661643939363866
          3838346232316434360a633964643434633763336565386339643764616566383661373663333062
          34316463363061633363623339383864626162323661396262303562656433343233
```

Then when you execute it, just pass in:

```
sudo ./ansible/bin/ansible-playbook -i hosts.yml hummingbird.yml --vault-id @prompt
```
