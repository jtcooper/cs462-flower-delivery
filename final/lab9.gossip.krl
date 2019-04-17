ruleset lab9.gossip {
  meta {
    shares __testing, peers, messages, getSequenceNumber, getPeer, 
      prepareMessage, prepareRumorMessage, prepareSeenMessage,
      getScheduledEvents
    use module io.picolabs.subscription alias Subscriptions
  }
  global {
    __testing = { "queries":
      [ { "name": "__testing" },
        {"name": "messages"},
        {"name": "peers"},
        {"name": "getSequenceNumber", "args": ["messageId"]},
        {"name": "getPeer"},
        {"name": "prepareMessage", "args": ["peerId"]},
        {"name": "prepareRumorMessage", "args": ["peerId"]},
        {"name": "prepareSeenMessage"},
        {"name": "getScheduledEvents"}
      ] , "events":
      [
        {"domain": "gossip", "type": "heartbeat"},
        {"domain": "gossip", "type": "add_peer", "attrs": ["Tx", "Tx_host"]},
        {"domain": "gossip", "type": "process", "attrs": ["status"]},
        {"domain": "gossip_test", "type": "add_message", 
            "attrs": ["storeId", "sequenceNumber", "temperature"]},
        {"domain": "gossip_test", "type": "stop_heartbeat"},
        {"domain": "gossip_test", "type": "reset_peers"}
      ]
    };
    
    timeInterval = function() {
      ent:timeInterval.defaultsTo(2)
    };
    peers = function() {
      ent:peers.defaultsTo({})
    };
    messages = function() {
      ent:messages.defaultsTo({})
    };
    process = function() {
      ent:process.defaultsTo(true);
    };
    
    /**
     * Logic for choosing a peer and whether to send a rumor or seen message
     */
    getPeer = function() {
      (peers().keys().length() == 0) => null  // no peers
          | selectPeerOnMissingCount()
    };
    
    selectPeerOnMissingCount = function() {
      numPeers = peers().keys().length();
      messageCounts = peers().map(function(value, key) {peerMissingMessagesFlat(value).length()})
        .klog("Missing message counts by peer:");
      highestMissing = messageCounts.values().sort("numeric")[numPeers - 1];
      // create an array of peers for which the missing count equals highestMissing
      // this is so that we can randomly choose one of them
      priorityPeers = messageCounts.keys().filter(function(peerId) 
          {messageCounts{peerId} == highestMissing}).klog("Peers with most missing:");
      // pick one of the high-missing-count peers randomly
      priorityPeers[random:integer(priorityPeers.length() - 1)]
    };
    
    peerMissingMessagesFlat = function(peer) {  // NOTE: returns an array of messages, rather than a map
      // flatten to a 1d array
      peerMissingMessages(peer)
        .values().reduce(function(a,b){a.append(b.values())}, []);
    }
    
    peerMissingMessages = function(peer) {
      // we have to parse a 2d map, where the 1st dim is storeId and 2nd dim 
      //   is the list of messages for that store
      
      messages().map(function(value, key) {
        // key: ORIGIN_ID
        // value: {ORIGIN_ID:SEQUENCE_NUMBER: map, ...}
        value.filter(function(valueInner, keyInner) {
          // keyInner: ORIGIN_ID:SEQUENCE_NUMBER
          // valueInner: {messageId: string, storeId: string, temperature: string, timestamp: date}
          sequenceNumber = getSequenceNumber(keyInner);
          // if peer doesn't contain key, return true; if it does contain key,
          //   check: if sequenceNumber is greater than count for peer{key}, return true
          not (peer >< key) || sequenceNumber > peer{key}
        })
      }).filter(function(value, key) {value.keys().length() > 0}) // filter out stores which have no missing messages
      .klog("missing messages for peer:");
    };
    
    getSequenceNumber = function(messageId) {
      // returns sequenceNumber; storeId is thrown away
      // extract returns array: [0]: sequenceNumber
      messageId.extract(re#\:(.*)#)[0]
    };
    
    prepareMessage = function(peerId) {
      // randomly choose the needed message type
      // if the peer doesn't have any missing messages, then always do a seen message
      hasMissingMessages = (peerMissingMessages(peers(){peerId}).length() > 0) => 1 | 0;
      (random:integer(hasMissingMessages) == 0) =>  
          {"type": "seen",  "data": prepareSeenMessage()} | 
          {"type": "rumor", "data": prepareRumorMessage(peerId)};
    };
    
    prepareRumorMessage = function(peerId) {
      // pick a random message to send which the peer hasn't seen yet
      // the random message should be the first of 
      messagesByStore = peerMissingMessages(peers(){peerId});
      
      // pick a first missing message randomly
      arrayStoreIds = messagesByStore.keys();
      storeIndex = random:integer(arrayStoreIds.length() - 1);
      storeMessages = messagesByStore{arrayStoreIds[storeIndex]};
      // pick the first one from that store
      storeMessages.values().sort(function(a, b) {
        aSeq = getSequenceNumber(a{"messageId"});
        bSeq = getSequenceNumber(b{"messageId"});
        aSeq < bSeq => -1 | aSeq == bSeq => 0 | 1
      }).head()
    };
    
    prepareSeenMessage = function() {
      // send the peer a map of all the farthest messages this node has seen
      messages().map(function(value, key) {storeHighestSequence(value, key)})
          .filter(function(value, key) {not value.isnull()}).klog("Sending message:");
    };
    
    storeHighestSequence = function(messages, storeId) {
      // determine the highest continuous sequence number for the passed storeId
      // if there's a gap, we want the sequence number right before the gap
      // we're assuming messages start at sequence 0
      // storeId: i.e. ORIGIN_ID
      sequenceNumbers = messages.keys()
        .map(function(key) {getSequenceNumber(key).as("Number")})
        .sort("numeric")
        .klog("store " + storeId + " sequence numbers:");
      // pick highest continuous sequence number
      compareRange = 0.range(sequenceNumbers.length() - 1);
      compareRange.filter(function(val) {sequenceNumbers[val] == val})
        .klog("Continuous numbers:").sort("ciremun").head();
    }
    
    findSub = function(eci) {
      // get the subscription for the peer that sent us a message
      Subscriptions:established().defaultsTo({})
        .filter(function(sub) {sub{"Rx"} == eci}).head();
    }
    
    getScheduledEvents = function() {
      schedule:list();
    };
  }
  
  /* --- END GLOBAL --- */
  
  rule gossip_heartbeat {
    select when gossip heartbeat
    pre {
      a = klog("--- HEARTBEAT ---");
      peer = getPeer().klog("selected peer:");
      message = prepareMessage(peer).klog("message:");
      // get subscription (verify that it still exists)
      subcription = Subscriptions:established().defaultsTo({})
        .filter(function(sub) {sub{"Tx"} == peer}).head().klog("subscription:");
    }
    if subcription then every {
      event:send({
        "eci": peer, 
        "eid": 0,
        "domain": "gossip", 
        "type": message{"type"},
        "attrs": message});
      send_directive("heartbeat", {"peer": peer, "sent_message": message});
    }
    fired {
      // if it was a rumor message, update the counter for the peer
      ent:peers := (message{"type"} == "rumor") =>
          peers().put([peer, message{"data"}{"storeId"}], getSequenceNumber(message{"data"}{"messageId"}).as("Number"))
              .klog("updated peers:") |
          peers();
    } finally {
      schedule gossip event "heartbeat" at time:add(time:now(), {"seconds": timeInterval()});
    }
  }
  
  rule gossip_seen {
    select when gossip seen where process() == true
    pre {
      a = event:attrs.klog("--- GOSSIP SEEN ---");
      subscription = findSub(meta:eci).klog("subscription:");
      peer = subscription{"Tx"}.klog("peer:");
      missingMessages = peerMissingMessagesFlat(event:attr("data")); // logged by function call
      // pick highest sequence number for each ORIGIN_ID
      //   this looks exactly like we're preparing a "seen" message
      seenByPeer = prepareSeenMessage().klog("messages seen after sending:");
      updatedPeersMap = peers().set(peer, seenByPeer);
    }
    if subscription && missingMessages.length() > 0 then noop()
    fired {
      ent:peers := updatedPeersMap.klog("updated peers map:");
      raise gossip event "send_rumors"
        attributes {"peer": peer, "messages": missingMessages};
    }
  }
  
  /**
   * Called after a gossip:seen event; send each missing rumor to the peer
   */
  rule gossip_send_rumors {
    select when gossip send_rumors
      foreach event:attr("messages") setting (message)
    pre {
      peer = event:attr("peer");
      rumorMessage = {"type": "rumor", "data": message}.klog("sending rumor message:");
    }
    event:send({
      "eci": peer, 
      "eid": 0,
      "domain": "gossip", 
      "type": rumorMessage{"type"},  // should be "rumor", but just for good design principle
      "attrs": rumorMessage})
  }
  
  /**
   * Accept new messages and save them to this object's block of messages. Also
   * update the sender's counts for which messages have been seen, so we don't
   * resend them back to the sender.
   */
  rule gossip_accept_rumor {
    select when gossip rumor where process() == true
    pre {
      a = event:attrs.klog("--- GOSSIP RUMOR ---");
      message = event:attr("data").klog("message:");
      subscription = findSub(meta:eci).klog("subscription:");
      peer = subscription{"Tx"}.klog("peer:");
      
      sequence = getSequenceNumber(message{"messageId"}).as("Number")
          .klog("message's sequence number:");
      currentStoreSequence = peers(){peer}{message{"storeId"}}.as("Number")
          .klog("peer's currently recorded store sequence:");
          
      // if message doesn't exist, this will be null
      containsMessage = messages().get([message{"storeId"}, message{"messageId"}]);
      
      updatedMessages = messages().put([message{"storeId"}, message{"messageId"}], message);
      updatedPeers = currentStoreSequence && currentStoreSequence + 1 != sequence => peers() |
          peers().put([peer, message{"storeId"}], sequence).klog("updated peers:");
    }
    if not containsMessage then noop();
    fired {
      ent:messages := updatedMessages.klog("updated messages:");
      ent:peers := updatedPeers;
      log info "successfully added message";
      // raise an event for the driver to perform some action on a new message
      raise order event "bid"
        attributes message;
    }
  }
  
  /**
   * Process event; set to on or off. When off, this node will stop processing 
   * all messages received.
   */
  rule set_process_state {
    select when gossip process
    pre {
      setState = event:attr("status") == "off" => false | true;
    }
    send_directive("set_process_state", {"process": setState});
    fired {
      ent:process := setState;
    }
  }
  
  /**
   * Manually subscribe to the passed node, where "Tx" is the "wellKnown_Tx" of
   * the peer.
   */
  rule add_peer_to_network {
    select when gossip add_peer where event:attr("Tx")
    pre {
      host = (event:attr("Tx_host") == "" || not event:attr("Tx_host")) => null 
        | event:attr("Tx_host")
      params = {
        "wellKnown_Tx": event:attr("Tx"),
        // "name": event:attr("name"),
        "name": meta:picoId,
        "channel_type": "subscription",
        "Rx_role": "node",
        "Tx_role": "node",
        "Tx_host": host
      }
    }
    noop();
    fired {
      raise wrangler event "subscription"
        attributes params
    }
  }
  
  /**
   * Adds to subscribed peers entity variable. Only selects if the subscription 
   * roles are both "node". Note that this rule is raised by both the sender 
   * and receiver of the subscription request.
   */
  rule complete_peer_subscription {
    select when wrangler subscription_added where event:attr("Tx_role") == "node" 
      && event:attr("Rx_role") == "node"
    pre {
      a = event:attrs.klog("Subscription success for: ")
      // b = event:attr("name").klog("Subscription success for: ")
    }
    noop();
    fired {
      ent:peers := peers().put(event:attr("Tx"), {});
    }
  }
  
  /**
   * Auto-accept policy for subscription requests if both roles are "node".
   */
  rule autoAccept {
    select when wrangler inbound_pending_subscription_added where event:attr("Tx_role") == "node" 
      && event:attr("Rx_role") == "node"
    pre{
      sub_attributes = event:attrs.klog("subcription: ").put("name", meta:picoId)
    }
    always{
      raise wrangler event "pending_subscription_approval"
          attributes sub_attributes;
      log info "Auto-accepted subcription for \"node\" roles.";
    }
  }
  
  /**
   * Add messages when driver receives an order broadcast event.
   */
  rule add_message {
    select when gossip_test add_message
    pre {
      storeId = event:attr("storeId")
      sequenceNum = event:attr("sequenceNumber")
      messageId = storeId + ":" + sequenceNum
      orderId = event:attr("orderId")
      storeLocation = event:attr("storeLocation")
      message = {"messageId": messageId, "storeId": storeId, "orderId": orderId, 
          "storeLocation": storeLocation}
    }
    always {
      ent:messages := messages().put([storeId, messageId], message);
    }
  }
  
  /* --- TESTING/ADMIN METHODS --- */
  
  rule stop_heartbeat {
    select when gossip_test stop_heartbeat
      foreach schedule:list() setting (event)  // just to be safe, stop all scheduled events
    pre {
      b = event.klog("stopping scheduled event:");
    }
    schedule:remove(event{"id"});
  }
  
  rule reset_peers {
    select when gossip_test reset_peers
    pre {
      peers = Subscriptions:established().defaultsTo({})
        .filter(function(sub) {sub{"Rx_role"} == "node" && sub{"Tx_role"} == "node"})
        .map(function(sub) {sub{"Tx"}})
        .reduce(function(a, b) {
            a.put(b, {})  // conver to a ma, where the value of each is an empty map as well
          }, {}).klog("updated peers list:");
    }
    always {
      ent:peers := peers;
    }
  }
  
  rule reset {
    select when driver reset
    always {
      ent:process := true;
      ent:messages := null;
      raise gossip_test event "reset_peers";
    }
  }
}
