ruleset delete_schedules {

  meta {
    shares __testing, get_scheduled
  }

  global {
    __testing = {
      "queries": [ { "name": "__testing" }, {"name": "get_scheduled"} ],
      "events": [ { "domain": "schedule", "type": "delete_scheduled_item",
                            "attrs": [ "id"] }]
    }

    get_scheduled = function() {
      schedule:list()
    }
  }

  rule delete {
    select when schedule delete_scheduled_item
    pre {
      id = event:attr("id")
    }
    schedule:remove(id)

  }


}
