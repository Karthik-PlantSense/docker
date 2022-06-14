from pyflink.common import WatermarkStrategy, Row
from pyflink.common.serialization import Encoder
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FileSink, OutputFileConfig, NumberSequenceSource
from pyflink.datastream.functions import RuntimeContext, MapFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.serialization import JsonRowDeserializationSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer,FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema, SerializationSchema,JsonRowSerializationSchema,Encoder
from pyflink.common.typeinfo import Types
import json
class MyMapFunction(MapFunction):

    def open(self, runtime_context: RuntimeContext):
        state_desc = ValueStateDescriptor('count', Types.INT())
        self.count_state = runtime_context.get_state(state_desc)

    def map(self, value):
        cnt = self.count_state.value()
        print("cnt: ",type(cnt),type(value[1]),cnt==value[1])
        if cnt is None:
            self.count_state.update(value[1])
            return Row(value[0],value[1])
        else:
            total_count = cnt+value[1]
            self.count_state.update(total_count)
            return  Row(value[0],total_count)

def state_access_demo():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(1000)

    # advanced options:

    # set mode to exactly-once (this is the default)
    # env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)

    # the sql connector for kafka is used here as it's a fat jar and could avoid dependency issues
    env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka-1.15.0.jar")

    deserialization_schema = JsonRowDeserializationSchema.builder().type_info(
                             type_info=Types.ROW_NAMED(
                             ["tag","count"], [Types.STRING(), Types.INT()])).build()


# {"tag":"Temp1","count":1}{"abc": "123", "xyz": "ddd"}


    kafka_consumer = FlinkKafkaConsumer(
        topics='test_source_topic',
        deserialization_schema=deserialization_schema,
        properties={'bootstrap.servers': '192.168.1.6:9092', 'group.id': 'test_group'})

    ds = env.add_source(kafka_consumer)
    ds.print()
    ds = ds.key_by(lambda a:a[0]) \
            .map(MyMapFunction(),output_type=Types.ROW([Types.STRING(), Types.INT()]))

    ds.print()


    serialization_schema = JsonRowSerializationSchema.builder().with_type_info(
        type_info=Types.ROW_NAMED(["tag","Total_Count"],[Types.STRING(), Types.INT()])).build()

    kafka_producer = FlinkKafkaProducer(
        topic='test_sink_topic',
        serialization_schema=serialization_schema,
        producer_config={'bootstrap.servers': '192.168.1.6:9092', 'group.id': 'test_group'})

    ds.add_sink(kafka_producer)
    
    env.execute('state_access_demo')


if __name__ == '__main__':
    state_access_demo()

    

