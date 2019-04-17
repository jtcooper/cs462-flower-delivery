ruleset use_google {
  meta {
    shares __testing
    use module io.picolabs.lesson_keys
    use module google alias google
        with api_key = keys:google{"api_key"}
  }
  global {
    __testing = { "queries":
      [ { "name": "__testing" }
      //, { "name": "entry", "args": [ "key" ] }
      ] , "events":
      [ //{ "domain": "d1", "type": "t1" }
        {"domain": "google", "type": "distance", "attrs": ["origin", "dest"]}
      //, { "domain": "d2", "type": "t2", "attrs": [ "a1", "a2" ] }
      ]
    }
  }
  
  rule calculate_distance {
    select when google distance
    pre {
      distance = google:calculate_distance(event:attr("origin"),
                              event:attr("dest")
                              ).klog("Distance:");
    }
    send_directive("distance", {"distance": distance})
  }
}
