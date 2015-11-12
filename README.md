reveal-media-webservice
=======================

Exposes the media analysis and retrieval API of the REVEAL project.
The project is made of two main packages:

### it.unimi.di.law : the crawler
- The code is a copy of the [BUbiNG source code][1] 
- The related [paper][2]
- And a [poster][3]
- More information about the crawler configuration [here][4]
- BUbiNG is also published on [maven central][5]. The reason we are not using the maven dependency and just a copy of the code instead, is that it was necessary to modify many classes to fit our needs and this was not possible by just extending . Some of the core code had to be changed, and especially the classes in the parse and store sub-packages.

The specific configuration properties of the crawler are defined in the ```reveal.properties``` file, which can be found in the ```resources``` folder of the project. For more details about the configuration, you should have a look at the BUbiNG documentation. 

### gr.iti.mklab.reveal : the REST API. 
The main class which exposes all the functionalities is the ```RevealController```. In the constructor, several objects are initialized, as well as the configuration in the following line: ```Configuration.load(getClass().getResourceAsStream("/remote.properties"));```

Choose one of the following files:
- remote.properties for the iti-310 configuration
- local.properties for the localhost configuration
- docker.properties for the docker configuration

#### Properties configuration

The properties file defines the following generic configuration settings:
- crawlsDir: The directory where the crawler stores its files (queue, metadata, persisted state)
- visualDir: The directory where the visual indexer stores its files
- learningFolder: The directory where the visual indexer can find the learning files
- indexServiceHost: Where the [visual indexing webservice is hosted][7]
- mongoHost: Where MongoDB is hosted
- getSocialMedia: True to also crawl for social media using the [simmo-stream-manager web service][6]
- manipulationReportPath: Where the forensics component's output is saved
- publish: If true, the output is published to RabbitMQ
- numCrawls: The maximum number of simultaneous crawls


#### Architecture

The ```RevealAgent``` controls and initializes the crawler and all necessary additional components, the visual indexing module for instance. Every new crawler instance starts in a new Thread and is independant of the other instances. The BUbiNG crawler itsels is a JMX agent, which means that all instances can be configured and monitored at runtime using jconsole. Although starting the crawler instance is equivalent to a new thread creation, stopping uses the dedicated JMX ```stop()``` command.

The ```CrawlQueueController``` is responisble for starting new crawl jobs, stopping running crawl jobs and generally for monitoring the crawler queue. Every crawl job starts at ```WAITING``` state. The ```CrawlQueueController``` checks the queue every 1 minute or whenever a new crawl job is submitted. If there is an empty slot, if the currently running crawls are less then the numCrawls property specified above, a new crawl thread is launched. Until the initilization is finished the crawl job is in ```STARTING``` state, this might last a couple of minutes and the crawl job should not be stopped at this point. The next step is the ```RUNNING``` state. A crawl job can be stopped or deleted. Deletion means that the collected data and the visual index are also deleted. The termination also takes a couple of minutes so during this time the crawl job is in ```STOPPING``` or ```DELETING``` state. Deleted crawl jobs disappear from the queue but stopped ones still stay there in ```FINISHED``` state.

Important note:  The whole text package is obsolete. Now we are using iit.demokritos maven dependency instead. I'm just leaving the code as a reference but it can as well be deleted.

The crawler has the possibility of also crawling social media by using the [simmo-stream-manager web service][6]. This can be configured in the properties file, as mentioned before.

[1]:  http://law.di.unimi.it/software.php#bubing
[2]:  http://www.quantware.ups-tlse.fr/FETNADINE/papers/P4.8.pdf
[3]:  http://wwwconference.org/proceedings/www2014/companion/p227.pdf
[4]:  http://law.di.unimi.it/software/bubing-docs/overview-summary.html
[5]:  https://search.maven.org/#artifactdetails|it.unimi.di.law|bubing|0.9.11|jar
[6]:  https://github.com/MKLab-ITI/simmo-stream-manager
[7]:  https://github.com/MKLab-ITI/multimedia-webservice
