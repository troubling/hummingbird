CLIs and SDKs
=============

Hummingbird is API compatible with OpenStack Swift, so most client command line tools and software development kits will work unchanged against a Hummingbird cluster.


## CLIs - Command Line Interfaces

Hummingbird comes with the Nectar client, built-in as the subcommand `hummingbird nectar` and at https://github.com/troubling/nectar as its own codebase.

Since it is OpenStack API compatible, you can also use tools like https://github.com/gholt/swiftly and the https://github.com/openstack/python-swiftclient default `swift` tool.

In addition, the hummingbird executable has many other subcommands useful while developing, such as `hummingbird nodes` to list devices for items. Run `hummingbird` with no parameters for full help text.


## SDKs - Software Development Kits

Once again, Nectar offers a new SDK for Go located at https://github.com/troubling/nectar which you can build your own tools from. The Nectar CLI tool is an example of how to do this.

Other OpenStack Swift SDKs should work perfectly fine with Hummingbird as well, such as https://github.com/gholt/swiftly http://gophercloud.io/docs/object-storage/ and the default https://github.com/openstack/python-swiftclient Python SDK.


## Benchmarking

Nectar comes with some basic benchmarking tools. Run `nectar` or `hummingbird nectar` with no additional parameters for full help text. `bench-put` `-get` `-post` `-delete` `-mixed` etc.

In addition, `hummingbird bench` is also included for a slightly different way of benchmarking a cluster.
