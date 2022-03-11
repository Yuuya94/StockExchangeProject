# StockExchangeProject

Architecture Big Data Project from IMT Atlantique


Made by : Thierry JIAO and Xinyan DU

## Description

An in-depth paragraph about your project and overview of use.

## Getting Started

### Dependencies

* Python
* Docker & docker-compose

### Installing

First, please install the python packages :

```bash
$ pip install -r requirements.txt
```

Please install the docker containers by executing these commands :

```bash
$ cd docker
$ sudo docker-compose up -d
```

You can check the status of the containers :

```bash
$ sudo docker ps
```

More info :

You can configure the number of spark workers adding parameter :

```bash
$ sudo docker-compose up --scale spark-worker=3 -d
```

You can also verify that cassandra is running :

```bash
$ cqlsh localhost -u cassandra -p cassandra
```

### Executing program

* How to run the program
* Step-by-step bullets
```
code blocks for commands
```

## Help

Any advise for common problems or issues.
```
command to run if program contains helper info
```

## Authors

Contributors names and contact info

ex. Dominique Pizzie  
ex. [@DomPizzie](https://twitter.com/dompizzie)

## Version History

* 0.2
    * Various bug fixes and optimizations
    * See [commit change]() or See [release history]()
* 0.1
    * Initial Release

## License

This project is licensed under the [NAME HERE] License - see the LICENSE.md file for details

## Acknowledgments

Inspiration, code snippets, etc.
* [awesome-readme](https://github.com/matiassingers/awesome-readme)
* [PurpleBooth](https://gist.github.com/PurpleBooth/109311bb0361f32d87a2)
* [dbader](https://github.com/dbader/readme-template)
* [zenorocha](https://gist.github.com/zenorocha/4526327)
* [fvcproductions](https://gist.github.com/fvcproductions/1bfc2d4aecb01a834b46)
