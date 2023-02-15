#include "kafkaConsumer.h"


bool run=true;
bool exit_eof=true;

string msg_consume(RdKafka::Message *message, void *opaque)
{
	std::string result;
	switch (message->err())
	{
	case RdKafka::ERR__TIMED_OUT:
		break;

	case RdKafka::ERR_NO_ERROR:
		/* Real message */
		// std::cout << "Read msg at offset " << message->offset() << std::endl;
		// if (message->key())
		// {
			// std::cout << "Key: " << *message->key() << std::endl;
		// }
		// printf("%.*s\n", static_cast<int>(message->len()), static_cast<const char*>(message->payload()));

		char buff[1000];
		snprintf(buff, static_cast<int>(message->len()), "%.*s", static_cast<int>(message->len()), static_cast<const char *>(message->payload()));
		result = buff;
		return result;
		break;
	case RdKafka::ERR__PARTITION_EOF:
		/* Last message */
		if (exit_eof)
		{
			run = false;
			cout << "ERR__PARTITION_EOF" << endl;
		}
		break;
	case RdKafka::ERR__UNKNOWN_TOPIC:
	case RdKafka::ERR__UNKNOWN_PARTITION:
		std::cerr << "Consume failed: " << message->errstr() << std::endl;
		run = false;
		break;
	default:
		/* Errors */
		std::cerr << "Consume failed: " << message->errstr() << std::endl;
		run = false;
	}
	return "errors";
}