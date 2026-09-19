import json
import os
from datetime import datetime

import requests
from pyflink.common import Duration, Types, WatermarkStrategy
from pyflink.common.time import Time
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat
from pyflink.datastream.window import SlidingEventTimeWindows


def parse(line):
    event = json.loads(line)
    event["timestamp_ms"] = int(
        datetime.fromisoformat(
            event["event_time"].replace("Z", "+00:00")
        ).timestamp()
        * 1000
    )
    return event


class EventTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, event, record_timestamp):
        return event["timestamp_ms"]


def send_to_hbase(item):

    import base64
    import requests

    product_id, clicks = item

    hbase_host = os.getenv("HBASE_HOST", "hbase")
    hbase_port = os.getenv("HBASE_REST_PORT", "8080")

    row_key = base64.b64encode(product_id.encode()).decode()
    column = base64.b64encode(b"info:clicks").decode()
    value = base64.b64encode(str(clicks).encode()).decode()

    payload = {
        "Row": [
            {
                "key": row_key,
                "Cell": [
                    {
                        "column": column,
                        "$": value
                    }
                ]
            }
        ]
    }

    url = f"http://{hbase_host}:{hbase_port}/product_alerts/{product_id}"

    try:
        response = requests.put(
            url,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=5
        )

        print(
            f"HBASE ALERTA: produto={product_id}, "
            f"clicks={clicks}, status={response.status_code}, "
            f"resposta={response.text}"
)

    except Exception as error:
        print(f"ERRO AO ENVIAR PARA HBASE: {error}")

    return item


env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

source = FileSource.for_record_stream_format(
    StreamFormat.text_line_format(),
    os.getenv("EVENT_FILE", "/data/events.jsonl"),
).monitor_continuously(Duration.of_seconds(2)).build()

events = (
    env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "json-file",
    )
    .map(parse)
)

watermarks = (
    WatermarkStrategy.for_bounded_out_of_orderness(
        Duration.of_seconds(10)
    )
    .with_timestamp_assigner(EventTimestampAssigner())
)

alerts = (
    events.assign_timestamps_and_watermarks(watermarks)
    .filter(lambda event: event["event_type"] == "click")
    .map(
        lambda event: (event["product_id"], 1),
        output_type=Types.TUPLE(
            [Types.STRING(), Types.INT()]
        ),
    )
    .key_by(lambda item: item[0])
    .window(
        SlidingEventTimeWindows.of(
            Time.minutes(1),
            Time.seconds(20),
        )
    )
    .reduce(
        lambda left, right: (
            left[0],
            left[1] + right[1],
        )
    )
    .filter(
        lambda item: item[1]
        >= int(os.getenv("TREND_THRESHOLD", "5"))
    )
)

alerts.map(send_to_hbase).print("ALERTA_PRODUTO_EM_ALTA")

env.execute("ecommerce-trending-products")

