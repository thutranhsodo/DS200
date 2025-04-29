import threading
from data_sender import run_streaming_server
from data_receiver import StreamingData
import warnings


if __name__ == "__main__":
    warnings.filterwarnings("ignore")
    
    streaming_rf = StreamingData(max_batches=10)

    server_thread = threading.Thread(target=run_streaming_server)
    streaming_thread = threading.Thread(target=streaming_rf.start_streaming)

    server_thread.start()
    streaming_thread.start()
    
    server_thread.join()
    streaming_thread.join()