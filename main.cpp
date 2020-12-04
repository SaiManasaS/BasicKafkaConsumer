#include <string>
#include <iostream>
#include <vector>

#include "..\include\librdkafka\rdkafkacpp.h"
#include "..\include\librdkafka\rdkafka.h"

#define ENABL_AGENT_DBG_VERBOSE

using namespace std;
using namespace RdKafka;

#ifdef ENABL_AGENT_DBG_VERBOSE
#define DBG(x) (x)
#else 
#define DBG(x) do{}while(0)
#endif

void pollForMsgs(rd_kafka_t * rk1, rd_kafka_t * rk2)
{
	rd_kafka_resp_err_t err, err2;
	string inputKafkaMsg, inputKafkaMsg2;

	DBG(cout << "Inside pollForMsgs()" << endl);

	// The while(1) loop
	while (1) //running?
	{
		rd_kafka_message_t* rkmessage = rd_kafka_consumer_poll(rk1, 500);
		rd_kafka_message_t* rkmessage2 = rd_kafka_consumer_poll(rk2, 500);
		DBG(cout << "Inside while()" << endl);

		if (rkmessage)
		{
			DBG(cout << "Dbg: Line 691" << endl);
			if ((rkmessage == NULL))
			{
				DBG(cout << "Dbg: Line 694" << endl);
				continue;
			}

			if ((rkmessage->err))
			{
				if ((rkmessage->err == RD_KAFKA_RESP_ERR__PARTITION_EOF))
				{
					DBG(cout << "Dbg: Line 702" << endl);
					continue;
				}

				if ((rkmessage->err == RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION ||
					rkmessage->err == RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC))
				{
					DBG(cout << "Dbg: Line 711" << endl);
					continue;
				}
				continue;
			}
			DBG(cout << "Dbg: Line 716" << endl);
			inputKafkaMsg = inputKafkaMsg + (char*)rkmessage->payload;

			rd_kafka_message_destroy(rkmessage);
		}
		else
		{
			DBG(cout << "Dbg: Line 728" << endl);
			DBG(cout << "inputKafkaMsg: " << inputKafkaMsg << endl);
			inputKafkaMsg = "";
		}

		// Second consumer message poller
		if (rkmessage2)
		{
			if ((rkmessage2 == NULL))
			{
				continue;
			}

			if ((rkmessage2->err))
			{
				if ((rkmessage2->err == RD_KAFKA_RESP_ERR__PARTITION_EOF))
				{
					DBG(cout << "Dbg: Line 709" << endl);
					continue;
				}

				if ((rkmessage2->err == RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION ||
					rkmessage2->err == RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC))
				{
					DBG(cout << "Dbg: Line 718" << endl);
					continue;
				}

				continue;
			}
			DBG(cout << "Dbg: Line 768" << endl);
			inputKafkaMsg2 = inputKafkaMsg2 + (char*)rkmessage2->payload;

			rd_kafka_message_destroy(rkmessage2);
		}
		else
		{
			DBG(cout << "Dbg: Line 779" << endl);
			//inputKafkaMsg2 = "{\"actionServiceComputer\":null,\"actionServiceIp\":\"0\",\"actionserviceAction\":3}__TypeId__Ncom.fim.action.dto.ActionDTOKafkaFormat";
			DBG(cout << "inputKafkaMsg2: " << inputKafkaMsg2 << endl);

			inputKafkaMsg2 = "";
		}
	}

	err = rd_kafka_consumer_close(rk1);
	err2 = rd_kafka_consumer_close(rk2);

	if (err || err2)
	{
		fprintf(stderr, "%% Failed to close consumer(s): %s\n", rd_kafka_err2str(err));
	}
	else
	{
		fprintf(stderr, "%% Consumer(s) closed\n");
	}
}

void subscribeToKafkaTopics(rd_kafka_t * rk, rd_kafka_topic_partition_list_t * topics)
{
	rd_kafka_resp_err_t err;

	DBG(cout << "Inside subscribeToKafkaTopics()" << endl);

	if ((err = rd_kafka_subscribe(rk, topics)))
	{
		fprintf(stderr, "%% Failed to start consuming topics: %s\n", rd_kafka_err2str(err));
		exit(1);
	}
}

int configKafkaConsumer()
{
	char hostname[128] = "localhost:9092";

	char errstr[512] = "";
	string kafkaBrokerServer = "localhost:9092";
	string kafkaBrokerServerList = "localhost:9092,52.140.103.217:9092";

	vector<string> firstListtopics;
	vector<string> secondListtopics;

	rd_kafka_topic_partition_list_t* list_firstListtopics = NULL;
	rd_kafka_topic_partition_list_t* list_secondListtopics = NULL;

	// Consumer Configurations
	rd_kafka_conf_t* config[2] = { NULL, NULL };

	// Kafka handles configured (as consumers)
	rd_kafka_t* rk[2] = { NULL, NULL };

	unsigned int i = 0;

	DBG(cout << "Inside config_kafka_consumer()" << endl);

	firstListtopics.push_back("firsttopic1");
	secondListtopics.push_back("secondtopic1");

	// Add topic to the first topic list
	list_firstListtopics = rd_kafka_topic_partition_list_new((int)firstListtopics.size());

	for (unsigned int i = 0; i < firstListtopics.size(); i++)
	{
		rd_kafka_topic_partition_list_add(list_firstListtopics, firstListtopics[i].c_str(),
			RD_KAFKA_PARTITION_UA);
	}

	// Add topic to the second topic list
	list_secondListtopics = rd_kafka_topic_partition_list_new((int)secondListtopics.size());

	for (unsigned int i = 0; i < secondListtopics.size(); i++)
	{
		rd_kafka_topic_partition_list_add(list_secondListtopics, secondListtopics[i].c_str(),
			RD_KAFKA_PARTITION_UA);
	}

	for (i = 0; i < 2; i++)
	{
		config[i] = rd_kafka_conf_new();

		// Configure the conf properties 'client.id' to identify the consumer here
		if (rd_kafka_conf_set(config[i], "client.id", hostname,
			errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
		{
			fprintf(stderr, "%% %s\n", errstr);
			DBG(cout << "client.id:" << endl);
			exit(1);
		}

		// Configure the conf properties 'group.id' to identify the consumer here
		if (rd_kafka_conf_set(config[i], "group.id", "foo",
			errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
		{
			fprintf(stderr, "%% %s\n", errstr);
			DBG(cout << "group.id:" << endl);
			exit(1);
		}

		// "localhost:9092,52.111.222.203:9092"
		// Configure the broker list here "host1:9092 host2:etc ..."
		if (rd_kafka_conf_set(config[i], "bootstrap.servers", "localhost:9092,52.140.103.217:9092",
			errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
		{
			fprintf(stderr, "%% %s\n", errstr);
			DBG(cout << "bootstrap.servers" << endl);
			exit(1);
		}

		// Create consumer handles
		if (!(rk[i] = rd_kafka_new(RD_KAFKA_CONSUMER, config[i],
			errstr, sizeof(errstr))))
		{
			fprintf(stderr, "%% Failed to create new consumer: %s\n", errstr);
			exit(1);
		}
	}
	subscribeToKafkaTopics(rk[0], list_firstListtopics);
	subscribeToKafkaTopics(rk[1], list_secondListtopics);

	pollForMsgs(rk[0], rk[1]);


	// destroy lists
}

void main()
{
	configKafkaConsumer();
	while (1);
}