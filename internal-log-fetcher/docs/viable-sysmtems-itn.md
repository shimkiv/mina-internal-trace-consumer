# Viable Systems ITN documentation

## **Introduction**

The main goal is to be able to see all the Incentivized testnet (ITN) nodes and their internal traces in one centralised place. For this purpose, we developed an app called Fetcher that discovers all the nodes in the ITN and fetches all the internal traces from each node and then displays it on a frontend app. With this approach, we will gain detailed insight into the functioning of the network and identify possible issues or bottlenecks.

Below, we describe how we simulated the upcoming ITN and also how the Fetcher discovers nodes and gathers traces. 

## **Setup**

**Nodes**

To simulate the upcoming network, we set up 200 mina nodes running in our Kubernetes cluster. These nodes are set up in a way that enables us to collect useful data and internal traces. To achieve this setup, the following flags should be added to each node on startup:



* internal-tracing: enables internal tracing in the node
* itn-keys: a list of public keys (base64 encoded ed25519) that are authorized to access the special ITN graphQL port.
* itn-graphql-port: Opens up the special graphQL on this specified port. Through this port, we can query traces from the node.
* uptime-submitter-key: Node-specific private key that is used to authenticate with the uptime service. 
* uptime-url: URL for the uptime service
* node-status-url: URL for a service to which the node sends its current states regularly (this is a different service than the uptime service)
* node-error-url: URL for an error collection service, when an error occurs on the node, it sends it to this service. 

**Tools and Apps**

The tools and apps used are:



* uptime service: Accepts uptime information from each running mina node (IP, graphQL port, uptime data)
* Google Cloud bucket storage: The uptime service stores each submission here
* fetcher app: The main app. It handles the discovery of the nodes and fetches the traces
* specialized openmina frontend: Displays all the nodes on the ITN, the blocks they received/created and the internal traces.
* Google sheet: A sheet with a list of whitelisted public keys used for uptime service authentication

(a diagram would be useful here to show how all the tools and services communicate with each other)

## **Node discovery**

A crucial part of our tooling is the fact that we see all the running nodes in the ITN. This means our fetcher app has to discover all said nodes. In this section we describe the discovery process and also the tools that are used in it.

**Uptime service**

This is a tool developed by the O(1) labs team to gather uptime information from running mina nodes. Mina nodes sends the uptime information in the form of submission messages to the uptime service every 15 minutes. 

The uptime service requires authentication via a pair of public and secret keys. These keys have the same format as Mina account keys. Each node has its own set of keys. In other words, the uptime service only accepts submission messages from nodes that have signed the message with a whitelisted public key.

The whitelist resides in a google sheet that the uptime service periodically downloads every 10 minutes. This list contains the public keys for all the 200 running nodes. 

This submission includes the nodes’ IP, the specialised graphQL port and other uptime information. What we need for node discovery is the IP, graphQL port and the time and date this information was posted.

All the valid submissions are then stored in a Google cloud bucket, sorted into a directory by date. The submissions in the bucket are json files named in the following format:  \
&lt;timestamp>-&lt;public_key>.json

ex.:

2023-06-29T00:00:20Z-B62qiUXd4qvpuq3nnL9t9QXsDdWArL5eceJNXL2XMSpdwmAoNWF43AB.json

**Discovery process**

The fetcher periodically queries the Google bucket and filters out the submissions that have been submitted in the last 15 minutes. A node is considered to be running, and hence up, when it has a valid submission in the last 15 minutes.

From the resulting submissions, the fetcher extracts the remote_address and graphql_control_port, the latter being the specialised ITN graphQL port. 

## **Fetching the internal traces**

With all the nodes discovered, the fetcher now periodically pulls the internal traces from the nodes. 

However, connecting and sending queries to the specialised ITN graphQL endpoint of the Mina node requires authentication. This authentication is done by sending an authentication message with the node signed by an ed25519 keypair. This keypair is fed to the Mina node on startup in the previously described flag itn-keys. 

After authentication, the fetcher is able to query the endpoint for the internal traces.

The internal traces are then processed and structurized and served for each node separately on a port opened for each node by a separate process called consumer. 

## **Serving the internal traces**

Now with all the traces collected, we need to serve them to the user. This is done using a modified version of the OpenMina frontend. 

**OpenMina frontend**

The frontend app queries the Fetcher app to get a list of all running nodes. This way, the frontend receives a list of nodes and their port on the Fetcher app. Then the frontend queries each consumer process’ endpoint on the Fetcher app to grab the structurized traces for each node and display it on a dashboard. 

By default, the front end opens up on the **Dashboard** tab:

![alt_text](/internal-log-fetcher/docs/resources/dashboard.png "Dashboard")


By default this displays all connected nodes, the number of which is displayed in the upper right corner of the screen. To filter out a particular node, click on the **All Nodes** buttom in the upper right corner of the screen to search for a node.

You can browse through different **Global [Slots](https://docs.minaprotocol.com/glossary#slot)** (which are slots irrespective of epochs in Mina).

To the right are two buttons:

**Show latency from fastest/Show latency from second fastest**

**Show active nodes/Show all nodes**

Below these buttons is a list of connected nodes.

From left to right, the column describe the following:

**Name** - The name of the node (it’s address).

**Status** - whether it is synced with the rest of the network.

**Candidate** - The candidate block the node is currently working with.

**Height** - the block height at which it currently is.

**Datetime** - The time and date when they were added

**Block Application** - How long did block application take

**Source** - The source of the block, can be either external (network), internal (from the node itself).

**Trace status** - Whether tracing was successfully implemented with this node


### Tracing

The Tracing page is an overview of calls made from various checkpoints within the Mina code. It informs us which processes are particularly slow, or, vice versa, performing very well.


![alt_text](/internal-log-fetcher/docs/resources/tracing_1.png "Overview")


Please note that this page displays tracing information for one particular node, the name or address of which is displayed in the upper right corner of the screen.

You can also see its **status** (whether it is synced with the network) as well as when the node was synced.

The first screen is the **Overview **tab in which you can see a visualization of the metrics for various checkpoints, represented by graphs.

By default, the graphs are sorted **Slowest first** since we are most interested in particularly slow processes. You can click on **Fastest first** to sort in the reverse order.

Let’s take a closer look at a checkpoint graph:

![alt_text](/internal-log-fetcher/docs/resources/tracing_2.png "Checkpoint")


At the top, we have the name for the checkpoint (**Verify transaction snarks done**).

Below the name is the total time of all calls made from that checkpoint (**1.33s**), as well as the number of calls made (**588 calls**). Whenever the node reaches the _Verify transactions snarks done _checkpoint, it will make a call to the front end.

The y axis represents the total time of all calls made after reaching that checkpoint.

The x axis represents the duration of calls made after reaching that checkpoint.

![alt_text](/internal-log-fetcher/docs/resources/tracing_3.png "Tooltip")


Hovering the cursor above a vertical row displays a tooltip that shows:



* How many calls were made within that **Range**
* Their **Mean** (average) duration
* The **Max** duration of a call within that range
* How many **Calls** were made 
* The cumulative **Total Time** of these calls

Squares marked with gray edges denote that the calls have an adequate duration. Yellow edges signify that the calls take slightly longer than usual, orange edges are for calls that are longer than usual, and red squares denote that the duration is too long and that the code near this checkpoint should be optimized.

We can also display the overview tab in compact mode, to get a clearer picture of which checkpoint calls are particularly slow. Click on the icon in the far right upper corner of the screen to switch to compact mode:


![alt_text](/internal-log-fetcher/docs/resources/tracing_4.png "Compact mode")


Clicking on a checkpoint call will expand it and reveal the full graph:


![alt_text](/internal-log-fetcher/docs/resources/tracing_5.png "Compact mode expand")

