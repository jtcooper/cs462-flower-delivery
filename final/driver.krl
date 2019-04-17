ruleset driver {
  meta {
    shares __testing, driver_id
    use module io.picolabs.wrangler alias wrangler
    use module io.picolabs.subscription alias Subscriptions
    use module google
  }
  global {
    __testing = { "queries":
      [ { "name": "__testing" },
        {"name": "driver_id"}
      //, { "name": "entry", "args": [ "key" ] }
      ] , "events":
      [
        {"domain": "order", "type": "broadcast", "attrs": ["storeId", "sequenceNumber", "orderId"]},
        {"domain": "driver", "type": "add_store", "attrs": ["Tx", "Tx_host"]},
        {"domain": "driver", "type": "location", "attrs": ["location"]},
        {"domain": "order", "type": "pickup"},
        {"domain": "order", "type": "delivered"}
      ]
    }
    
    driver_id = function() {
      wrangler:channel()
        .filter(function(channel) {channel{"name"} == "main"})
        .head(){"id"};
    }
    
    current_order = function() {
      ent:current_order.defaultsTo(null);
    };
    
    location = function() {
      ent:location.defaultsTo("500 W Center St Provo, UT");
    };
    
    get_order_manager = function() {
      Subscriptions:established("Tx_role", "order_manager").head();
    };
  }
  
  /**
   * Driver receives an order from a store pico and propagates the order to 
   * other drivers in the network.
   */
  rule receive_order_broadcast {
    select when order broadcast
    always {
      raise order event "bid"
        attributes event:attrs;
      // propagate order out to other known drivers through gossip; we'll do 
      // this by adding a message to the gossip ruleset
      raise gossip_test event "add_message"
        attributes event:attrs;
    }
  }
  
  /**
   * Driver determines whether it wants to submit a bid to a store; if it does,
   * then submit.
   */
  rule submit_order_bid {
    select when order bid
    pre {
      storeLocation = event:attr("storeLocation");
      distance = google:calculate_distance(location(), storeLocation);
    }
    
    if not current_order() then
      event:send(
        { "eci": event:attr("storeId"), "eid": "bid_order", "domain": "order", "type": "bid",
          "attrs": {"orderId":event:attr("orderId"), "driverId": driver_id(), "distance": distance}
        });
  }
  
  /**
   * Driver receives an order assignment from a store and determines whether
   * to accept the assignment.
   */
  rule receive_order_assignment {
    select when pickup assigned
    if not current_order() then noop()
    fired {
      raise pickup event "confirmed"
        attributes event:attrs;
      ent:current_order := event:attr("orderId");
    }
  }
  
  /**
   * Driver submits a confirmation to the flower store that it is coming to
   * pick up.
   */
  rule order_assignment_accepted {
    select when pickup confirmed
    event:send(
      { "eci": event:attr("storeId"), "eid": "confirm_pickup", "domain": "pickup", "type": "confirmed",
        "attrs": {"orderId":event:attr("orderId"), "driverId": driver_id()}
      })
  }
  
  /**
   * Order has been picked up from the store; should now be enroute.
   */
  rule order_pickup {
    select when order pickup
    event:send(
      {
        "eci": get_order_manager(){"Tx"}.klog("order_manager eci:"), 
        "eid": "pickup_status", "domain": "order", "type": "pickup",
        "attrs": {"orderId": current_order()}})
  }
  
  /**
   * Driver has delivered the order to the customer and receives a rating.
   */
  rule order_delivered {
    select when order delivered
    event:send(
      {
        "eci": get_order_manager(){"Tx"}, "eid": "pickup_status", "domain": "order", "type": "delivered",
        "attrs": {"orderId": current_order()}
      })
    fired {
      ent:current_order := null;
    }
  }
  
  /**
   * Subscribing to stores. Manually subscribe to the passed store, where "Tx" 
   * is the "wellKnown_Tx" of the store.
   */
  rule add_store_subscription {
    select when driver add_store where event:attr("Tx")
    pre {
      host = (event:attr("Tx_host") == "" || not event:attr("Tx_host")) => null 
        | event:attr("Tx_host")
      params = {
        "wellKnown_Tx": event:attr("Tx"),
        "name": meta:picoId,
        "channel_type": "subscription",
        "Rx_role": "driver",
        "Tx_role": "store",
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
   * Auto-accept policy for store subscription requests.
   */
  rule auto_accept {
    select when wrangler inbound_pending_subscription_added where event:attr("Tx_role") == "store" 
      || event:attr("Tx_role") == "order_manager"
    pre{
      sub_attributes = event:attrs.klog("subcription: ").put("name", meta:picoId)
    }
    always{
      raise wrangler event "pending_subscription_approval"
          attributes sub_attributes;
      log info "Auto-accepted subcription for new store in network.";
    }
  }
  
  rule set_location {
    select when driver location
    always {
      ent:location := event:attr("location");
    }
  }
  
  rule reset {
    select when driver reset
    always {
      ent:current_order := null;
    }
  }
}
