# Deploy your own Plasma Chain

## Prerequisites 
- `node.js` -- version 11.6.0
- `git` -- version 2.17.1
- Build essentials or similar (specifically `g++` -- version 7.3.1)

## Setup
To deploy a new Plasma Chain, use the following commands:
```
0) $ npm install plasma-chain [-g]  # install the plasma chain operator. Global flag is optional, if you don't
use global, just replace all of the following commands with `npm run plasma-chain [command]`. If you can't install
globally without `sudo` then just use local!

1) $ plasma-chain account new  # create a new account

2) # On Rinkeby testnet, send your new Operator address ~0.5 ETH.
You can use a faucet to get test ETH for free here: https://faucet.rinkeby.io/

2.5) $ plasma-chain list # list all the plasma chains which others have deployed to the Plasma Network Registry 

3) $ plasma-chain deploy # deploy a new Plasma Chain.
Note you will be prompted for a unique Plasma Chain name & IP address.
If you are running on your laptop, just set the IP to `0.0.0.0` as you probably don't
want to reveal your IP to the public. However, if you are running in a data center and would
like to accept Plasma transactions & serve a block explorer to the public, go ahead and set an IP.

4) $ plasma-chain start # start your new Plasma Chain
You can also view your local block explorer at http:127.0.0.1:8000

5) [optional]
Open a new terminal. In this new terminal use the following command:
$ plasma-chain testSwarm # spam your Plasma Chain with tons of test transactions 😁

```

### To open your port, you may need to forward traffic from port 80 to port 3000
See: https://coderwall.com/p/plejka/forward-port-80-to-port-3000

## Docker Setup
It is also possible to run the operator in Docker via `docker-compose`.

To run the operator in Docker you will (for now) need to checkout the source, so you can build the container yourself.

To build the container execute `docker-compose build`.

After that the steps are same as described in _Setup_ above (starting from step 1) with `plasma-chain` replaced by `docker-compose run --rm --service-ports operator`.

side note: Since the current setup requires you to enter your passphrase via the commandline and has multiple steps to it, it
is not possible to simply run `docker-compose up`.