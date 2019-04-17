ruleset flower_store {
  meta {
    shares __testing, store_id, get_order_manager
    use module io.picolabs.lesson_keys
    use module io.picolabs.wrangler alias wrangler
    use module io.picolabs.subscription alias subscriptions
    use module google
  }
  global {
    __testing = { "queries":
      [ { "name": "__testing" },
       {"name": "store_id"},
      //, { "name": "entry", "args": [ "key" ] }
      {"name": "get_order_manager"}
      ] , "events":
      [ //{ "domain": "d1", "type": "t1" }
        {"domain": "order", "type": "new", "attrs": ["customerPhone", "customerLocation"]}
        
      //, { "domain": "d2", "type": "t2", "attrs": [ "a1", "a2" ] }
      ]
    }
    
    get_order_manager = function() {
      subscriptions:established("Tx_role", "order_manager").head()
    }
    
    current_bids = function() {
      ent:current_bids.defaultsTo({})
    }
    
    sequence_num = function() {
      ent:sequence_num.defaultsTo(0)
    }
    
    store_id = function() {
      //ent:store_id.defaultsTo(meta:picoId)
      wrangler:channel()
        .filter(function(channel) {channel{"name"} == "main"})
        .head(){"id"};
    }
    
    getLocationByStoreId = function() {
      //perhaps insert function that can randomly get a location from google map API?
      ent:location.defaultsTo("100 W Center St Provo, UT")
    }
    
    expectedDeliveryTime = function(order_id, driver_id) {
      time:add(time:now(), 
          {"seconds": (
              current_bids().get([order_id, driver_id]) + 
              google:calculate_distance(
                  getLocationByStoreId(), 
                  wrangler:skyQuery(get_order_manager(){"Tx"}, "order_manager", "getCustomerLocationByOrderId", 
                      {"orderId": order_id})
              ).klog("distance from store to customer:")).as("Number")
          });
    }
  }
  
  
  
  /**
   * Selects on an incoming new order and sends info to the drivers so
   * they can bid on it
   * This rule creates a UUID id for the order
   * Sends attrs "storeId" and "orderId" to the OrderManager
   */
  rule new_order {
    select when order new 
    pre {
      new_order_id = random:uuid()
      store_id = store_id()
    }
    always {
      raise order event "create" attributes {"orderId": new_order_id, "storeId": store_id, 
      "customerPhone": event:attr("customerPhone"), "customerLocation": event:attr("customerLocation")};
      raise order event "broadcast" attributes {"orderId": new_order_id, "storeId": store_id}
    }
  }
  
  /**
   * This rule tells the OrderManager to create a new order
   */
  rule create_order {
    select when order create
    pre {
      eci = get_order_manager(){"Tx"}
    }
    event:send(
      {"eci": eci, "eid": "new_order", "domain": "order", "type": "create",
        "attrs": {"orderId":event:attr("orderId"), "storeId": event:attr("storeId"),
        "customerPhone": event:attr("customerPhone"), "customerLocation": event:attr("customerLocation")
        }
      })
    
  }
  
  /**
   * This rule runs after a new order is made and the drivers need
   * to bid on the new order
   */
  rule broadcast_order {
    select when order broadcast
    
    foreach subscriptions:established("Tx_role", "driver") setting (subscription)
    pre {
      order_id = event:attr("orderId").klog()
    }
    event:send(
      {"eci": subscription{"Tx"}, "eid": "new_broadcast", "domain": "order", "type": "broadcast", 
        "attrs": {"orderId": order_id, "storeId": event:attr("storeId"), "sequenceNumber": sequence_num(), 
        "storeLocation": getLocationByStoreId()
        }
      })
    always {
      schedule pick event "bid" at time:add(time:now(), {"seconds": 15}) attributes {"orderId": order_id};
      ent:sequence_num := sequence_num() + 1;
      ent:current_bids := current_bids().put(order_id, {});
    }
  }
  
  rule pick_bid {
    select when pick bid
    pre {
      order_id = event:attr("orderId");
      b = order_id.klog("---Picking order bid for orderId:");
      // pick the closest driver
      order_bids = current_bids(){order_id}.defaultsTo({}).klog("Bids for order:");
      bid_array = order_bids.keys().reduce(function(a, b) {a.append({"id": b, "distance": order_bids{b}})}, [])
        .klog("Array of bids:")
        .sort(function(a, b) {
          aDist = a{"distance"};
          bDist = b{"distance"};
          aDist < bDist  => -1 |
          aDist == bDist =>  0 |
                             1
        }).klog("Sorted bids:");
      closest_driver = bid_array.head().klog("Selected driver:");
      driver_id = closest_driver{"id"}.klog("driver_id:");
      modified_bids = order_bids.delete(driver_id).klog("Modified bids for order:");
    }
    if driver_id then 
      noop()
    fired {
      raise pickup event "assigned" attributes {"orderId": order_id, "driverId": driver_id};
      ent:current_bids := current_bids().put(order_id, modified_bids);
    }
  }
  
  /**
   * This rule runs when a driver sends a bid to the store. 
   * Receives the driversId and the orderId
   */
  rule bid_order {
    select when order bid
    pre {
      b = klog("Received new order bid")
      distance = event:attr("distance")
      driver_id = event:attr("driverId").klog("DriverId: ")
      order_id = event:attr("orderId").klog("OrderId: ")
      drivers_map = current_bids(){order_id}.defaultsTo({}).put(driver_id, distance);
    }
    noop()
    always {
      ent:current_bids := current_bids().put(order_id, drivers_map);
    }
  }
  
  /**
   * This rule runs when a bid has been selected. It will broadcast the event
   * to all the drivers
   * Sends the orderId and storeId to the drivers
   */
  rule pickup_assigned {
    select when pickup assigned
    pre {
      order_id = event:attr("orderId").klog("Sending assignment message for order:")
      driver_id = event:attr("driverId").klog("Driver id:")
    }
    event:send(
      {"eci": driver_id, "eid": "driver_assigned", "domain": "pickup", "type": "assigned",
        "attrs": {"orderId": order_id, "storeId": store_id()}
      })
    always {
      schedule pick event "bid" at time:add(time:now(), {"seconds": 10}) 
        attributes {"orderId": order_id, "driverId": driver_id}
    }
  }
  
  /**
   * This rule runs when a driver has confirmed pickup of the order
   * Receives driverDistance from the driver
   */
  rule pickup_confirmed {
    select when pickup confirmed
    
    //We could possibly update the delivery time here if need be
    pre {
      order_id = event:attr("orderId").klog("order_id:")
      driver_id = event:attr("driverId").klog("driver_id:")
      id_to_remove = schedule:list().klog("Schedule: ")
          .filter(function(x) {x{["event", "attrs", "orderId"]} == order_id}).head().klog("selected event:")
          {"id"}.klog("event id:")
    }
    schedule:remove(id_to_remove)
    always {
      raise order event "assigned" attributes {"orderId": order_id, "driverId": driver_id};
    }
  }
  
  /**
   * This rule runs when an pickup assignment is confirmed and the order needs 
   * to be updated in the OrderManager.
   * Sends the driverId, orderId, and expectedDeliveryTime to the OrderManager
   * Receives driver Location and/or distance in minutes from store
   */
  rule assign_order {
    select when order assigned
    pre {
      driver_id = event:attr("driverId")
      order_id = event:attr("orderId")
      eci = get_order_manager(){"Tx"}
      expectedDeliveryTime = expectedDeliveryTime(order_id, driver_id);
    }
    event:send(
      {"eci": eci, "eid": "order_assigned", "domain": "order", "type": "assigned",
        "attrs": {"orderId": order_id, "driverId": driver_id, "expectedDeliveryTime": expectedDeliveryTime.klog()}
      })
  }
  
  /**
   * Subscribing to drivers. Manually subscribe to the passed driver, where "Tx" 
   * is the "wellKnown_Tx" of the driver.
   */
  rule add_driver_subscription {
    select when driver add_driver where event:attr("Tx")
    pre {
      host = (event:attr("Tx_host") == "" || not event:attr("Tx_host")) => null 
        | event:attr("Tx_host")
      params = {
        "wellKnown_Tx": event:attr("Tx"),
        "name": meta:picoId,
        "channel_type": "subscription",
        "Rx_role": "store",
        "Tx_role": "driver",
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
   * Auto-accept policy for driver subscription requests.
   */
  rule autoAccept {
    select when wrangler inbound_pending_subscription_added where event:attr("Tx_role") == "driver" 
      && event:attr("Rx_role") == "store"
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
    select when store location
    always {
      ent:location := event:attr("location");
    }
  }
  
}
