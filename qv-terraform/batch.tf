resource "aws_iam_role" "aws_batch_service_role" {
  name = "aws_batch_service_role-${var.env_name}"

  assume_role_policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Action": "sts:AssumeRole",
            "Effect": "Allow",
            "Principal": {
                "Service": "batch.amazonaws.com"
            }
        }
    ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "aws_batch_service_role" {
  role       = "${aws_iam_role.aws_batch_service_role.name}"
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSBatchServiceRole"
}

resource "aws_iam_role" "ecs_instance_role" {
  name = "ecs_instance_role-${var.env_name}"

  assume_role_policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Action": "sts:AssumeRole",
            "Effect": "Allow",
            "Principal": {
                "Service": "ec2.amazonaws.com"
            }
        }
    ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "ecs_instance_role" {
  role       = "${aws_iam_role.ecs_instance_role.name}"
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role"
}

resource "aws_iam_instance_profile" "ecs_instance_role" {
  name = "ecs_instance_role-${var.env_name}"
  role = "${aws_iam_role.ecs_instance_role.name}"
}


resource "aws_batch_compute_environment" "qv" {
  compute_environment_name = "qv-costbuilder-${var.env_name}"

  compute_resources {
    instance_role = "${aws_iam_instance_profile.ecs_instance_role.arn}"

    instance_type = [
      "${var.batch_instance_type}",
    ]

    max_vcpus = "${var.batch_max_vcpus}"
    min_vcpus = "${var.batch_min_vcpus}"

    security_group_ids = [
      "${aws_security_group.qv_neo4j.id}",
    ]

    subnets = [
      "${aws_subnet.pri_subnet.id}",
    ]

    type = "EC2"
  }

  service_role = "${aws_iam_role.aws_batch_service_role.arn}"
  type         = "MANAGED"

}

resource "aws_batch_job_queue" "qv_job_queue" {
  name                 = "qv_jb_queue-${var.env_name}"
  state                = "ENABLED"
  priority             = "1"
  compute_environments = ["${aws_batch_compute_environment.qv.arn}"]
}
