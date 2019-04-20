data "aws_ami" "neo4j" {
  most_recent = true
  owners      = ["679593333241"]

  filter {
    name   = "name"
    values = ["neo4j-community-1-3.5.3-*"]
  }

  filter {
    name   = "root-device-type"
    values = ["ebs"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

resource "aws_security_group" "qv_neo4j" {
  name        = "neo4j inbound"
  description = "security group settings for neo4j server"
  vpc_id      = "${aws_vpc.qv.id}"
}

resource "aws_security_group_rule" "allow_ssh" {
  type              = "ingress"
  from_port         = 22
  to_port           = 22
  protocol          = "tcp"
  cidr_blocks       = ["${var.ingress_cidr}"]
  security_group_id = "${aws_security_group.qv_neo4j.id}"
}

resource "aws_security_group_rule" "allow_web_port" {
  type              = "ingress"
  from_port         = 7473
  to_port           = 7473
  protocol          = "tcp"
  cidr_blocks       = ["${var.ingress_cidr}"]
  security_group_id = "${aws_security_group.qv_neo4j.id}"
}

resource "aws_security_group_rule" "allow_db_con" {
  type              = "ingress"
  from_port         = 7687
  to_port           = 7687
  protocol          = "tcp"
  cidr_blocks       = ["${var.ingress_cidr}"]
  security_group_id = "${aws_security_group.qv_neo4j.id}"
}

resource "aws_security_group_rule" "egress_allow_all" {
  type              = "egress"
  from_port         = 0
  to_port           = 65535
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = "${aws_security_group.qv_neo4j.id}"
}

resource "aws_instance" "qv_neo4j" {
  ami                         = "${data.aws_ami.neo4j.id}"
  instance_type               = "${var.instance_type}"
  key_name                    = "${var.sshkey_name}"
  vpc_security_group_ids      = ["${aws_security_group.qv_neo4j.id}"]
  subnet_id                   = "${aws_subnet.pub_subnet.id}"
  associate_public_ip_address = true

  tags {
    Name = "Neo4j server ${var.env_name}"
  }
}
