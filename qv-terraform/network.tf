#VPC resources
resource "aws_vpc" "qv" {
  cidr_block       = "${var.vpc_cidr}"
  instance_tenancy = "${var.vpc_tenancy}"

  tags {
    Name = "QV-VPC-${var.env_name}"
  }
}

#Subnet resources
resource "aws_subnet" "pub_subnet" {
  vpc_id = "${aws_vpc.qv.id}"

  tags {
    Name = "QV-PubSubnet-${var.env_name}"
  }

  cidr_block        = "${var.pub_subnet_cidr}"
  availability_zone = "${var.pub_az}"
}

resource "aws_subnet" "pri_subnet" {
  vpc_id = "${aws_vpc.qv.id}"

  tags {
    Name = "QV-PriSubnet-${var.env_name}"
  }

  cidr_block        = "${var.pri_subnet_cidr}"
  availability_zone = "${var.pri_az}"
}

resource "aws_internet_gateway" "qv" {
  vpc_id = "${aws_vpc.qv.id}"

  tags {
    Name = "QV-igw-${var.env_name}"
  }
}

resource "aws_route_table" "pub_rt" {
  vpc_id = "${aws_vpc.qv.id}"

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = "${aws_internet_gateway.qv.id}"
  }

  tags {
    Name = "QV-PubRt-${var.env_name}"
  }
}

resource "aws_route_table_association" "public" {
  subnet_id      = "${aws_subnet.pub_subnet.id}"
  route_table_id = "${aws_route_table.pub_rt.id}"
}

resource "aws_eip" "qv" {
  vpc        = true
  depends_on = ["aws_internet_gateway.qv"]
}

resource "aws_nat_gateway" "qv" {
  allocation_id = "${aws_eip.qv.id}"
  subnet_id     = "${aws_subnet.pub_subnet.id}"
  depends_on    = ["aws_internet_gateway.qv"]

  tags {
    Name = "QV-NatGw-${var.env_name}"
  }
}

resource "aws_route_table" "pri_rt" {
  vpc_id = "${aws_vpc.qv.id}"

  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = "${aws_nat_gateway.qv.id}"
  }

  tags {
    Name = "VPC-PriRt-${var.env_name}"
  }
}

resource "aws_route_table_association" "private" {
  subnet_id      = "${aws_subnet.pri_subnet.id}"
  route_table_id = "${aws_route_table.pri_rt.id}"
}
