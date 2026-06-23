# brew tap hashicorp/tap
# brew install hashicorp/tap/terraform
#
# terraform version
#
# terraform init
#
# terraform plan
#
# terraform apply
#
# http://localhost:8080
#
# terraform destroy

# 1. Define the plugins (providers) needed to talk to your target API
terraform {
  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 3.0.1"
    }
  }
}

# 2. Initialize the provider instance
provider "docker" {}

# 3. Pull the official Nginx web server image from Docker Hub
resource "docker_image" "nginx_image" {
  name         = "nginx:latest"
  keep_locally = false
}

# 4. Instantiate a running container using that image
resource "docker_container" "nginx_server" {
  image = docker_image.nginx_image.image_id
  name  = "terraform-demo-webserver"

  # Map internal container port 80 to your machine's local port 8080
  ports {
    internal = 80
    external = 8080
  }
}