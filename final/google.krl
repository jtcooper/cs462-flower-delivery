ruleset google {
  meta {
    shares __testing
    use module io.picolabs.lesson_keys
    configure using api_key = keys:google{"api_key"}
    provides
        calculate_distance
  }
  global {
    __testing = { "queries":
      [ { "name": "__testing" }
      //, { "name": "entry", "args": [ "key" ] }
      ] , "events":
      [ //{ "domain": "d1", "type": "t1" }
      //, { "domain": "d2", "type": "t2", "attrs": [ "a1", "a2" ] }
      ]
    }
    
    calculate_distance = function(origin, dest){
      base_url = <<https://maps.googleapis.com/maps/api/distancematrix/json>>;
      response = http:get(base_url, qs = {
        "key":api_key,
        "origins":origin,
        "destinations":dest,
        "departure_time":"now",
        "units":"imperial"
      }).klog("Google response:");
      //response{"content"}.klog("content:"){"rows"}[0].klog("row 0:"){"elements"}[0].klog("element 0:"){"duration"}{"value"}
      response{"content"}
        .decode(){"rows"}.head()
        {"elements"}.head(){["duration", "value"]};
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
