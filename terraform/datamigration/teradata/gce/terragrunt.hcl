include "root" {
  path = find_in_parent_folders("tg_root.hcl")
}

dependency "pubsub" {
  config_path = "../pubsub"
  mock_outputs = {
    dtsagent_controller_sub_name = ""
  }
}

inputs = {
  dtsagent_controller_sub_name = dependency.pubsub.outputs.dtsagent_controller_sub_name
}
