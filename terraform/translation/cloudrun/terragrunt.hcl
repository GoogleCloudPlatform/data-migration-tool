include "root" {
  path = find_in_parent_folders("tg_root.hcl")
}

dependencies {
  paths = [
    "../gcc",
  ]
}

dependency "registry" {
  config_path = "../registry"
  mock_outputs = {
    dvt_image            = ""
    event_listener_image = ""
  }
}

inputs = {
  dvt_image            = dependency.registry.outputs.dvt_image
  event_listener_image = dependency.registry.outputs.event_listener_image
}
