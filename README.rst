Requirements
===========

- Install Java 8 sdk
- Install maven build system


Things to know
==============

- This template is supposed to be used inside the JMOAB, so you need to setup it first
- It will generate an uber jar
- It will be compliant to deploy to Marathon


Jargon
------

- mvn: It is the maven build system used by the JAVA MOAB to build and deploy projects
       As python projects are for now in the Java MOAB, we use it to launch commands needed to build this project

- jdk: Java developement kit, needed to be able to compile .java file into bytecode for the java virtual machine (jvm)

- jre: Java runtime environement, needed to execute your bytecode


Before Working
==============

  1) You need to setup a JMOAB environment. Follow this guide https://confluence.criteois.com/display/RP/JMOAB+-+Getting+Started

How to build
============

  1) One you have your project in the jmoab tree
        mvn clean package
    in order to test and build the project

  2) To generate the uber jar do a
        mvn clean package -DpackageForDeploy
     and you will find it in the ./target directory under the name *-uber.jar
     to run it
       java -jar target/*-uber.jar
     and you should see
       com.criteo.java.template.Main - Hello anonymous

  If you change the Main class do not forget to edit in the pom.xml this line
    <mainClass>com.criteo.java.template.Main</mainClass>


How to test
============
  1) mvn test
