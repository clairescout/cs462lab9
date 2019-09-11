ruleset gossip {

  meta {
    shares __testing, getPeer, prepareRumorMessage, update, temp_logs, prepareSeenMessage, idToRx, smart_tracker
  }

  global {

    __testing = { "queries": [ { "name": "__testing" },
                                {"name": "idToRx"},
                                {"name": "getPeer"},
                                {"name": "update", "args": ["state"]},
                                {"name": "prepareRumorMessage", "args": ["subscriberId"]},
                                {"name": "prepareSeenMessage"},
                                {"name": "temp_logs"},
                                {"name": "smart_tracker"}],
                "events": [ { "domain": "clear", "type": "variables" },
                            { "domain": "initialize", "type": "gossip" },
                            { "domain": "gossip", "type": "heartbeat"},
                            { "domain": "gossip", "type": "start_heartbeat"},
                            {"domain": "gossip", "type": "process",
                              "attrs": ["process"] },
                            { "domain": "wovyn", "type": "new_temperature_reading",
                            "attrs": [ "temperature", "timestamp"] },
                            { "domain": "subscription", "type": "create",
                            "attrs": [ "wellknownEci"] }] }

    getPeer = function() {
      bools_need_rumor = ent:picoId_to_rx.map(function(rx, subscriberId){
        seen = ent:temp_logs.map(function(messages, picoId) {
          logged_sequence_num = get_highest_sequence_number_from_key(picoId);
          tracked_val = ent:smart_tracker{[subscriberId, picoId]};
          val = (tracked_val == null || logged_sequence_num > tracked_val) => true | false;
          val;
        }).klog("seen");
        values = seen.values().klog("values");
        we_have_more = values.any(function(x) {x==true});
        we_have_more.klog("we have more");
        we_have_more;
      });
      bools_need_rumor.klog("BOOLS NEED RUMOR");
      filtered = bools_need_rumor.filter(function(val, subscriberId) { val == true });
      length = filtered.keys().length().klog("LENGTh");
      index = random:integer(length-1).klog("INDEX");
      pico_key = ent:picoId_to_rx.keys()[0].klog("PICO KEY");
      filter_key = filtered.keys()[index].klog("FILTER KEY");
      key = (length < 1 ) =>  pico_key | filter_key;
      key.klog("KEY");
      key;
    }

    prepareRumorMessage = function(subscriberId) {
      sequence_number_and_pico = sequence_number_and_picoId_for_given_pico_rumor_message(subscriberId).klog("pico and seq");
      picoId = sequence_number_and_pico{"picoId"};
      sequence_num = sequence_number_and_pico{"sequence_number"};
      messageId = picoId + ":" + sequence_num;
      message = ent:temp_logs{[picoId, messageId]};
      {
        "message": message,
        "sequence_number": sequence_num
      }
    }

    sequence_number_and_picoId_for_given_pico_rumor_message = function(subscriberId) {
      // logs = ent:smart_tracker{subscriberId}.klog("LOGS"); // TO DO CHANGE THIS TO LOOK THROUGH TEMP LOGS
      // bools = logs.map(function(sequence_num, picoID) {
      //   logged_sequence_num = get_highest_sequence_number_from_key(picoId);
      //   sequence_num = (logged_sequence_num > sequence_num) => true | false;
      //   sequence_num;
      // });
      logs = ent:temp_logs.map(function(messages, picoId) {
        logged_sequence_num = get_highest_sequence_number_from_key(picoId);
        smart_sequence_num = ent:smart_tracker{[subscriberId, picoId]};
        val = (smart_sequence_num == null || logged_sequence_num > smart_sequence_num) => true | false;
        val;
      }).klog("LOGS");

      head =  ent:temp_logs.keys().head().klog("Temp log key head");
      bools_filtered = logs.filter(function(v, k) { v == true }).klog("bools filtered");
      temp_head = ent:temp_logs.keys()[0].klog("KEY");
      length = bools_filtered.keys().length().klog("LENGTh");
      index = random:integer(length-1).klog("INDEX");
      filtered_key = bools_filtered.keys()[index];
      needs_message = (length > 0) => filtered_key | temp_head;
      obj = {
        "picoId": needs_message,
        "sequence_number": get_necessary_sequence_number_from_key(subscriberId, needs_message)//get_highest_sequence_number_from_key(needs_message)
      };
      obj;
    }

    prepareSeenMessage = function() {
      logs = ent:temp_logs.klog("temp logs: ");
      mapped = ent:temp_logs.map(function(val, key) {
        // get_highest_sequence_number(val)
        ent:smart_tracker{[meta:picoId, key]}
      });
      mapped;
    }

    get_highest_sequence_number = function(message_rumors) {
      length = message_rumors.length(); // TODO: make actually find the highest sequence number
      val = length - 1;
      val;
    }

    get_highest_sequence_number_from_key = function(key) {
      // values = ent:temp_logs{key};
      // get_highest_sequence_number(values)
      ent:smart_tracker{[meta:picoId, key]}
    }

    get_necessary_sequence_number_from_key = function(subscriber, key) {
      my_highest_val = get_highest_sequence_number_from_key(key).klog("my highest");
      their_highest_val = ent:smart_tracker{[subscriber, key]}.klog("their highest");
      sequence_num = (my_highest_val > their_highest_val) => (their_highest_val + 1) | my_highest_val;
      sequence_num_final = (sequence_num == "null1") => 0 | sequence_num;
      sequence_num_final;
    }



    send_rumor = defaction(subscriber, m) {
      eci = ent:picoId_to_rx{subscriber}.klog("ECI");
      // send a message to the peer
      event:send({
      "eci":eci, "eid": "message",
      "domain": "gossip", "type": "rumor",
      "attrs": {"message": m }
      })
    }

    send_seen = defaction(subscriber, m) {
      eci = ent:picoId_to_rx{subscriber}.klog("ECI");
      event:send({
      "eci": eci, "eid": "message",
      "domain": "gossip", "type": "seen",
      "attrs": {"message": m, "picoId": meta:picoId}
      })
    }

    generate_seen = function() {

    }

    smart_tracker = function() {
      ent:smart_tracker
    }

    temp_logs = function() {
      ent:temp_logs
    }

    idToRx = function() {
      ent:picoId_to_rx
    }
  }

  rule rumor {
    select when gossip rumor
    pre {
      m = event:attr("message");
      picoId = m{"SensorID"}
      messageId = m{"MessageID"}
      a = ent:temp_logs{picoId}.defaultsTo({})
      smart_tracker_val = ent:smart_tracker{[picoId, sensorId]}
      sequence_val = messageId.extract(re#:(\d+)#).decode()[0].klog("Sequence val")
    }
    if smart_tracker_val == null || (smart_tracker_val < sequence_val && (sequence_val - smart_tracker_val) < 2)  then noop()
    fired {
      ent:smart_tracker{[meta:picoId, picoId]} := sequence_val
    }
    finally {
      a = a.put([messageId], m).klog("A");
      ent:temp_logs{picoId} := a;
    }
  }

  rule seen {
    select when gossip seen
    foreach event:attr("message") setting(sequence_num, sensorId)
      pre {
        picoId = event:attr("picoId").klog("PICOID")
        sensId = sensorId.klog("SENSID")
        //temp_logs_contains = temp_log_contains(sequence_num, sensorId)
        smart_tracker_val = ent:smart_tracker{[picoId, sensorId]}.klog("SMART trakcer val")
      }
      if smart_tracker_val == null || smart_tracker_val < sequence_num then noop()
      fired {
        ent:smart_tracker{[picoId, sensorId]} := sequence_num
      } else {
        raise gossip event "send_rumor"
          attributes {"subscriber": picoId}
      }
    /*
    check for any rumors the pico knows about that are not in the seen
    message and send them (as rumors) to the pico that sent the seen event.
    */
  }

  rule heartbeat {
    select when gossip heartbeat where ent:process == "on"
    pre {
      subscriber = getPeer()
      message_type = random:integer(1);
    }
    if message_type == 1 then noop();
    fired {
      raise gossip event "send_rumor"
        attributes {"subscriber": subscriber}
    } else {
      raise gossip event "send_seen"
        attributes {"subscriber": subscriber}
    }

  }

  rule send_rumor_rule {
    select when gossip send_rumor
    pre {
      subscriber = event:attr("subscriber")
      m_obj = prepareRumorMessage(subscriber)
      m = m_obj{"message"};
      sequence_num = m_obj{"sequence_number"}
    }
    send_rumor(subscriber, m)
    always {
      ent:smart_tracker{[subscriber, m{"SensorID"}]} := sequence_num;
    }
  }

  rule send_seen_rule {
    select when gossip send_seen
    pre {
      subscriber = event:attr("subscriber")
      m = prepareSeenMessage()
      messageId = m{"MessageID"}
    }
    send_seen(subscriber, m);
    always {
      // ent:smart_tracker{[subscriber]} := m
    }
  }

  rule start_heartbeat {
    select when gossip start_heartbeat
    always {
      schedule gossip event "heartbeat" repeat "*/5  *  * * * *"     //at time:add(time:now(), {"seconds": 10})
    }

  }

  rule add_temperature_to_log {
    select when wovyn new_temperature_reading
    pre {
      temperature = event:attr("temperature")
      timestamp = event:attr("timestamp")
      picoId = meta:picoId
      messageId = (picoId+ ":" + ent:sequence_number)
      obj = {
        "MessageID": messageId,
        "SensorID": picoId,
        "Temperature": temperature,
        "Timestamp": timestamp
      }
      a = ent:temp_logs{picoId}.defaultsTo({})
    }

    always {
      a = a.put([messageId], obj);
      ent:sequence_number := ent:sequence_number.defaultsTo(0);
      ent:temp_logs{picoId} := a;
      ent:smart_tracker{[meta:picoId, picoId]} := ent:sequence_number;
      ent:sequence_number := ent:sequence_number + 1
    }
  }


  rule accept_gossip_subscriptions {
    select when wrangler inbound_pending_subscription_added
    pre {
      hi = event:attrs.klog("ATTRIBUTES IN PENDING");
      subscriptionHasAttrPicoId = event:attr("picoId").klog("Has Pico")
      picoId = event:attr("picoId")
      rx = event:attr("Rx")
      tx = event:attr("Tx")
    }
    if subscriptionHasAttrPicoId then
    noop()
    fired {
      ent:picoId_to_rx{[picoId]} := tx;
      ent:smart_tracker{[picoId]} := {};
      raise wrangler event "pending_subscription_approval" attributes event:attrs;
    }
  }

  rule subscription_added {
    select when wrangler subscription_added
    pre {
      attributess = event:attrs.klog("ATTRIBUTES")
      rx = event:attr("Rx").klog("Rx")
      tx = event:attr("Tx").klog("Tx")
      picoId = meta:picoId
    }
    event:send({
      "eci":tx, "eid": initialize,
      "domain": "gossip", "type": "picoId_rx",
      "attrs": {"picoId": picoId, "Rx": rx, "Tx": tx}
    })
  }

  rule add_picoId_to_rx {
    select when gossip picoId_rx
    pre {
      picoId = event:attr("picoId").klog("picoID")
      rx = event:attr("Rx").klog("rx")
      tx = event:attr("Tx").klog("tx")
    }
    if rx == tx then noop()
    notfired {
      ent:smart_tracker{[picoId]} := {};
      ent:picoId_to_rx{[picoId]} := rx;
    }
  }

  rule createSubscription {
    select when subscription create
    pre {
      picoId = meta:picoId.klog("Pico ID")
      node = "node"
      wellknownEci = event:attr("wellknownEci").klog("ECI")
    }
    always {
      raise wrangler event "subscription" attributes
         {
           "picoId" : picoId,
           "Rx_role": node,
           "Tx_role": node,
           "channel_type": "subscription",
           "wellKnown_Tx" : wellknownEci
         }
    }
  }

  rule set_process {
    select when gossip process
    pre {
      process = event:attr("process")
    }
    always {
      ent:process := process;
    }
  }

  rule initialize {
    select when initialize gossip
    always {
      ent:process := "on";
      ent:sequence_number := 0;
      ent:temp_logs := ent:temp_logs.defaultsTo({});
      ent:picoId_to_rx := ent:picoId_to_rx.defaultsTo({});
      ent:smart_tracker := ent:smart_tracker.defaultsTo({});
    }
  }

  rule clear_ent_variables {
    select when clear variables
    always {
      ent:process := "on";
      ent:sequence_number := 0;
      ent:temp_logs := {};
      ent:smart_tracker := {};
      // ent:picoId_to_rx := {}
    }
  }


}
