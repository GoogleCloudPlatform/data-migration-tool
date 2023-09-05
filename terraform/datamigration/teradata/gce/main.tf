/**
 * Copyright 2021 Google LLC.
 *
 * This software is provided as-is, without warranty or representation for any use or purpose.
 * Your use of it is subject to your agreement with Google.
 */
/************************* TERADATA TO GOOGLE BIGQUERY END TO END MIGRATION INFRASTRUCTURE*************************/

/******************************************
  Compute instance deployment
*****************************************/

/*Service Account creation that will be attached to the Compute Engine Agent VM */

locals {
  datamigration_teradata_vmstartup_script = "${var.datamigration_teradata_script}/vm_start_script.sh"
}

resource "google_service_account" "service_account" {
  project      = var.project_id
  account_id   = var.agentvm_sa
  display_name = "Service Account for GCE Agent VM and DTS"
}

/* Provide required IAM roles to Agent VM Service Account */

resource "google_project_iam_member" "agentsa_iam" {
  depends_on = [google_service_account.service_account]
  project    = var.project_id
  for_each   = toset(var.agentsa_roles)
  role       = each.value
  member     = "serviceAccount:${var.agentvm_sa}@${var.project_id}.iam.gserviceaccount.com"
}

/* Creating a NAT gateway and a Cloud Router in the provided VPC network with auto configurations
If you do not want to set up auto configured IPs for NAT and Cloud Router, please create it beforehand suiting
your network requirements and set the variable create_nat = false in variables.tf.
The Compute Engine Teradata Agent VM requires a NAT connection for start up script*/
resource "google_compute_router" "router" {
  depends_on = [google_project_iam_member.agentsa_iam]
  count      = var.create_nat ? 1 : 0
  name       = var.cloud_router
  region     = var.location
  network    = var.network

  bgp {
    asn = 64514
  }
}

resource "google_compute_router_nat" "nat" {
  depends_on                         = [google_compute_router.router]
  count                              = var.create_nat ? 1 : 0
  name                               = var.cloud_nat
  router                             = var.cloud_router
  region                             = var.location
  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"

  log_config {
    enable = true
    filter = "ERRORS_ONLY"
  }
}

#Create an Attached disk
resource "google_compute_disk" "dm-disk-terdata-bq" {
  depends_on = [google_compute_router_nat.nat]
  project    = var.project_id
  name       = var.disk_name
  type       = var.disk_type
  zone       = var.zone
  size       = var.disk_size
  labels = {
    "env" = "dm_teradata_bq"
  }
}

#Create an compute instance
resource "google_compute_instance" "dm-vm-teradata-bq" {
  depends_on   = [google_compute_disk.dm-disk-terdata-bq]
  name         = var.name
  zone         = var.zone
  machine_type = var.machine_type

  network_interface {
    network    = var.network
    subnetwork = var.subnetwork
  }

  boot_disk {
    initialize_params {
      image = var.image
      size  = var.boot_size

    }
    auto_delete = true
  }

  attached_disk {
    source = google_compute_disk.dm-disk-terdata-bq.id
  }

  service_account {
    # Google recommends custom service accounts that have cloud-platform scope and permissions granted via IAM Roles.
    email  = "${var.agentvm_sa}@${var.project_id}.iam.gserviceaccount.com"
    scopes = ["cloud-platform"]
  }

  metadata = {
    scripts-bucket = "${var.config_gcs_bucket}-${var.customer_name}"
    project-id     = "${var.project_id}"
    controller-sub = "${var.dtsagent_controller_sub_name}"
  }

  metadata_startup_script = file(local.datamigration_teradata_vmstartup_script)

  labels = {
    "env" = "dm_teradata_bq"
  }
}
