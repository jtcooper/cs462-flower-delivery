ruleset order_manager {
  meta {
    shares __testing, getCustomerLocationByOrderId
    use module io.picolabs.lesson_keys
    use module io.picolabs.twilio_v2 alias twilio
        with account_sid = keys:twilio{"account_sid"}
             auth_token =  keys:twilio{"auth_token"}
    
  }
  global {
    __testing = { "queries":
      [ { "name": "__testing" }
      //, { "name": "entry", "args": [ "key" ] }
      ] , "events":
      [ //{ "domain": "d1", "type": "t1" }
        {"domain": "order_manager", "type": "add_sub", "attrs": ["Tx", "Tx_host"]},
      //, { "domain": "d2", "type": "t2", "attrs": [ "a1", "a2" ] }
      ]
    }
    
    default_number = "+13259390035"
    
    //Possible state values for the order object
    CREATED = "created"
    PICKUP_ASSIGNED = "pickup assigned"
    ENROUTE = "enroute"
    DELIVERED = "delivered"
    FAILED = "failed"
    
    allOrders = function() {
      ent:all_orders.defaultsTo({})
    }
    
    getCustomerPhoneByOrderId = function(orderId) {
      ent:all_orders{[orderId, "customerPhone"]}
    }
    
    getCustomerLocationByOrderId = function(orderId) {
      ent:all_orders{[orderId, "deliveryLocation"]}
    }
    
  }
  
  /**
   * This rule runs when the store sends the order manager a new order
   * Receives attrs "storeId" and "orderId" from the flower_store
   * Possible receives deliveryLocation and customerPhone from the store as well
   */
  rule order_created {
    select when order create
    
    pre {
      order_id = event:attr("orderId")
      new_order = {"storeId": event:attr("storeId"), "state": CREATED, "expectedDeliveryTime": "",
        "driverId": "", "deliveryLocation": event:attr("customerLocation"),
        "customerPhone": event:attr("customerPhone")
      }
    }
    always {
      ent:all_orders := allOrders().put(order_id, new_order)
    }
  }
  
  /**
   * This rule runs when the store tells the order manager that a driver
   * has been assigned
   * Receives the "orderId", "driverId", and "expectedDeliveryTime" attrs from the flower_store
   */
  rule order_assigned {
    select when order assigned
    
    pre {
      order_id = event:attr("orderId")
      order_to_update = allOrders(){order_id}
      order_to_update{"driverId"} = event:attr("driverId")
      order_to_update{"expectedDeliveryTime"} = event:attr("expectedDeliveryTime").klog()
      order_to_update{"state"} = PICKUP_ASSIGNED
    }
    event:send({
      "eci":"3Gj12fQa7vgwuApUXZNEdh", 
      "eid":0, 
      "domain":"test", 
      "type":"new_message", 
      "attrs":{"to": getCustomerPhoneByOrderId(order_id), "from":default_number,"message":"Order state is " + PICKUP_ASSIGNED}
    })
    
    always {
      ent:all_orders := allOrders().put(order_id, order_to_update.klog("Updating the following order:"));
    }
    
  }
  
  /**
   * This rule runs when the driver says the order has been picked up
   * from the store
   * Receives the "orderId" attribute and then updates the state
   */
  rule order_pickup {
    select when order pickup
    pre {
      order_id = event:attr("orderId")
      order_to_update = allOrders(){order_id}
      order_to_update{"state"} = ENROUTE
    }
    event:send({
      "eci":"3Gj12fQa7vgwuApUXZNEdh", 
      "eid":0, 
      "domain":"test", 
      "type":"new_message", 
      "attrs":{"to": getCustomerPhoneByOrderId(order_id), "from":default_number,"message":"Order state is " + ENROUTE}
    })
    always {
      ent:all_orders := allOrders().put(order_id, order_to_update.klog("Updating the following order:"))
    }
  }
  
  /**
   * This rule runs when the order has been delivered
   * Receives the "orderId" attribute and then updates state
   * Completes the order process
   */
  rule order_delivered {
    select when order delivered
    pre {
      order_id = event:attr("orderId")
      order_to_update = allOrders(){order_id}
      order_to_update{"state"} = DELIVERED
    }
    event:send({
      "eci":"3Gj12fQa7vgwuApUXZNEdh", 
      "eid":0, 
      "domain":"test", 
      "type":"new_message", 
      "attrs":{"to": getCustomerPhoneByOrderId(order_id), "from":default_number,"message":"Order state is " + DELIVERED}
    })
    always {
      ent:all_orders := allOrders().put(order_id, order_to_update.klog("Updating the following order:"))
    }
  }
  
  /**
   * Subscribing to stores. Manually subscribe to the passed store, where "Tx" 
   * is the "wellKnown_Tx" of the store.
   */
  rule add_subscription {
    select when order_manager add_sub where event:attr("Tx")
    pre {
      host = (event:attr("Tx_host") == "" || not event:attr("Tx_host")) => null 
        | event:attr("Tx_host")
      params = {
        "wellKnown_Tx": event:attr("Tx"),
        "name": meta:picoId,
        "channel_type": "subscription",
        "Rx_role": "order_manager",
        "Tx_role": "peer",
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
    select when wrangler inbound_pending_subscription_added where event:attr("Tx_role") == "peer" 
      && event:attr("Rx_role") == "order_manager"
    pre{
      sub_attributes = event:attrs.klog("subcription: ").put("name", meta:picoId)
    }
    always{
      raise wrangler event "pending_subscription_approval"
          attributes sub_attributes;
      log info "Auto-accepted subcription for new peer in network.";
    }
  }
  
  
}
