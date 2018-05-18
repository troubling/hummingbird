Running tempest against HAIO
============================

Quick instructions

```
wget https://github.com/troubling/hummingbird/releases/download/v1.2.0/hummingbird
chmod +x hummingbird
./hummingbird init haio
./hummingbird-init-haio.sh

sudo apt-get install docker.io
git clone https://github.com/nadeemsyed/dockerized-keystone.git
cd dockerized-keystone/
make
sudo vim /etc/hummingbird/proxy-server.conf
(diff at https://github.com/nadeemsyed/dockerized-keystone/blob/master/README.md)
hball restart

git clone https://github.com/openstack/tempest.git
cd tempest
sudo apt-get install virtualenv
sudo apt-get install python-dev
virtualenv .venv
source .venv/bin/activate
pip install .
tempest init hbird
cp hbird/etc/tempest.conf.sample hbird/etc/tempest.conf
vim hbird/etc/tempest.conf
(diff at https://gist.github.com/corystone/a5bbafa6804c9278eb951c88127d634c)

tempest run --workspace hbird --regex object_storage
```
