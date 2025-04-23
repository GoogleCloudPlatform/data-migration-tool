include "root" {
  path = find_in_parent_folders("tg_root.hcl")
}

dependencies {
  paths = [
    "../gcs",
  ]
}

dependency "registry" {
  config_path = "../registry"
  mock_outputs = {
    dvt_image = ""
  }
}

inputs = {
  dvt_image = dependency.registry.outputs.dvt_image
}
