variable "credentials"{
    description = "My credential"
    default = "./keys/my-creds.json"
}

variable "project"{
    description = "Project"
    default = "neon-opus-412521"
}

variable "region"{
    description = "Project region"
    default = "europe-west2"
}

variable "location"{
    description = "Project location"
    default = "EU"
}

variable "bq_dataset_name"{
    description = "My BigQuery Dataset Nane"
    default = "demo_dataset_by_khumiso"
}

variable "gcs_bucket_name"{
    description = "My Storage Bucket Name"
    default = "neon-opus-412521-terra-bucket"
}

variable "gcs_storage_class"{
    description = "Bucket storage class"
    default = "STANDARD"
}

