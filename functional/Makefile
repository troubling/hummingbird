# To run against TempAuth:
#   export AUTH_URL=http://127.0.0.1:8080/auth/v1.0
#   export AUTH_USER=test:tester
#   export AUTH_KEY=testing
#   unset AUTH_TENANT
#   unset AUTH_PASSWORD

# To run against Keystone:
#   export AUTH_URL=http://192.168.56.1:5000/v3/
#   export AUTH_USER=tester
#   export AUTH_TENANT=test
#   export AUTH_PASSWORD=testing
#   unset AUTH_KEY

ifndef AUTH_URL
    export AUTH_URL=http://127.0.0.1:8080/auth/v1.0
    export AUTH_USER=test:tester
    export AUTH_KEY=testing
endif

test:
	HUMMINGBIRD_FUNCTIONAL_TESTS=yes go test
