Ansible notes
===========

Secrets
-----------
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
sudo ansible-playbook -i hosts.yml hummingbird.yml --vault-id @prompt
```


Keys and Certs
--------------

Two vars need to be placed in your inventory.

* ca\_key\_content: CA private key pem
* ca\_cert\_content: CA cert pem

It's highly encouraged to encrypt this file:

```
ansible-vault encrypt group_vars/hummingbird/ca.yml --ask-vault-pass
```

An example can be found in ansible/examples/ca.yml, which you can use (password: asdf):
```
mkdir group_vars/hummingbird
cp examples/ca.yml group_vars/hummingbird/
ansible-vault edit group_vars/hummingbird/ca.yml
ansible-vault rekey group_vars/hummingbird/ca.yml --ask-vault-pass
```


Generating the CA key:

```
openssl genrsa -out /path/to/key.pem 4096
```

Generate the self signed CA cert (from the hummingbird dir):

```
openssl req -config ansible/keys/ca.conf -key /path/to/key.pem -new -x509 -days 3560 -extensions ca_ext -out /path/to/ca.cert.pem -subj "/CN=Hummingbird CA"
```
