include "root" {
  path = find_in_parent_folders("tg_root.hcl")
}

dependencies {
  paths = [
    "../pubsub"
  ]
}
