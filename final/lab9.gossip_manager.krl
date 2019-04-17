ruleset lab9.gossip_manager {
  meta {
    shares __testing, children, sensors
    use module io.picolabs.wrangler alias wrangler
  }
  global {
    __testing = { "queries":
      [ { "name": "__testing" },
        {"name": "children"},
        {"name": "sensors"}
      //, { "name": "entry", "args": [ "key" ] }
      ] , "events":
      [ 
        {"domain": "gossip_test", "type": "start_all"},
        {"domain": "gossip_test", "type": "stop_all"},
        {"domain": "gossip_manager", "type": "create_child", "attrs": ["name"]},
        {"domain": "driver_manager", "type": "reset_children"},
      ]
    };
    
    children = function() {
      ent:children;
    };
    /**
     * Doesn't use the entity variable, but gets a list from the Wrangler instead
     */
    wranglerChildren = function() {
      wrangler:children().reduce(function(a, b) {a.put(b{"name"}, b)}, {}).klog("children:");
    };
    /**
     * @param children: caller decides whether to use Wrangler-created list or entity variable
     */
    rxs = function(children) {
      children.map(function(child) {
        {}.put(child{"name"}, 
            wrangler:skyQuery(child{"eci"}, "io.picolabs.wrangler", "channel", {"id": "wellKnown_Rx"})
                .filter(function(channel) {channel{"name"} == "wellKnown_Rx"}).head(){"id"});
      }).reduce(function(a, b) {a.put(b)}, {})
      .map(function(value, key) {value{key}}).klog("children wellKnown_Rxs:");
    };
    sensors = function() {
      // randomly generate three sensor ids, if not already created, and return the next sequence number
      ent:sensors.defaultsTo({});
    };
    numSensors = function() {
      sensors().length();
    }
    starting = function() {
      ent:starting.defaultsTo(false);
    }
  }
  
  rule delete_children {
    select when gossip_manager delete
      foreach children() setting (child)
    always {
      raise wrangler event "child_deletion"
        attributes {"name": child{"name"}};
      ent:children := {} on final;
      ent:sensors := null on final;
    }
  }
  
  rule start_all_heartbeats {
    select when gossip_test start_all
      foreach children().values() setting (node)
    pre {
      b = node{"name"}.klog("starting heartbeat for node:");
    }
    event:send({
      "eci": node{"eci"}, 
      "eid": 0,
      "domain": "gossip", 
      "type": "heartbeat"});
  }
  
  rule stop_all_heartbeats {
    select when gossip_test stop_all
      foreach children().values() setting (node)
    pre {
      b = node{"name"}.klog("stopping heartbeat for node:");
    }
    event:send({
      "eci": node{"eci"}, 
      "eid": 0,
      "domain": "gossip_test", 
      "type": "stop_heartbeat"});
  }
  
  rule create_child {
    select when gossip_manager create_child
    pre {
      nodeName = event:attr("name");
    }
    noop();
    fired {
      raise wrangler event "child_creation"
        attributes { "name": nodeName,
                     "color": "#c60d19",
                     "rids": ["io.picolabs.logging", "lab9.gossip"]};
    }
  }
  
  rule child_initialized {
    select when wrangler child_initialized where starting() == false
    pre {
      children = wranglerChildren();
      rxs = rxs(children);
      otherRxs = rxs.filter(function(value, key) {
        key != event:attr("name")
      }).klog("other Rxs (potential subscribers):");
      randomPeer = otherRxs.keys()[random:integer(otherRxs.keys().length() - 1)]
    }
    event:send({"eci": children{event:attr("name")}{"eci"}, "eid": 0, "domain": "gossip", "type": "add_peer",
          "attrs": {"Tx": rxs{randomPeer}}});
    fired {
      ent:children := children;
    }
  }
  
  rule reset_children {
    select when driver_manager reset_children
    pre {
      children = wranglerChildren();
    }
    always {
      raise driver_manager event "reset_child" attributes {"children": children};
      ent:children := children;
    }
  }
  
  rule reset_child {
    select when driver_manager reset_child
      foreach event:attr("children").values() setting (child)
    event:send({
      "eci": child{"eci"}, "eid": "reset", "domain": "driver", "type": "reset"})
  }
  
  rule stop_all_scheduled_events {
    select when driver_manager reset_children
      foreach schedule:list() setting (scheduledEvent)
    pre {
      b = scheduledEvent.klog("deleting scheduled event:");
    }
    schedule:remove(scheduledEvent{"id"})
  }
}
