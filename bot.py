import os

from dotenv import load_dotenv
from loguru import logger
from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat.processors.audio.audio_buffer_processor import AudioBufferProcessor
from pipecat.serializers.twilio import TwilioFrameSerializer
from pipecat.transports.websocket.fastapi import FastAPIWebsocketTransport, FastAPIWebsocketParams

from pipecat.services.openai.realtime.llm import OpenAIRealtimeLLMService

load_dotenv(override=True)


async def run_bot(websocket):
    """
    Builds and runs the full voice pipeline for a single call. Called 
    once per call, from server.py's /ws endpoint, right after
    Twilio's WebSocket connection is accepted.
    """

    import json
    _ = await websocket.receive_text()
    start_raw = await websocket.receive_text()
    call_data = json.loads(start_raw)
    stream_sid = call_data["start"]["streamSid"]
    call_sid = call_data["start"]["callSid"]

    logger.info(f"Call started -- stream_sid={stream_sid}, call_sid={call_sid}")

    # Connects raw Twilio Media Stream audio into Pipecat's internal audio frame format and vice versa
    serializer = TwilioFrameSerializer(
        stream_sid=stream_sid,
        call_sid=call_sid,
        account_sid=os.getenv("TWILIO_ACCOUNT_SID"),
        auth_token=os.getenv("TWILIO_AUTH_TOKEN"),
    )

    transport = FastAPIWebsocketTransport(
        websocket=websocket,
        params=FastAPIWebsocketParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
            # Detect interruption signals
            vad_analyzer=SileroVADAnalyzer(),
            serializer=serializer,
        ),
    )

    llm = OpenAIRealtimeLLMService(
        api_key=os.getenv("OPENAI_API_KEY"),
        settings=OpenAIRealtimeLLMService.Settings(
            model="gpt-realtime-2",
            system_instruction=(
            "You are roleplaying as a patient calling a medical scheduling line. "
            "Speak naturally and conversationally, like a real person on the phone. "
            "Not overly formal, occasional filler words are fine. "
            "Your specific scenario and goal for this call will be provided separately."
            ),
        ),
    )
    
    # Record the full call audio as it goes through the pipeline
    audiobuffer = AudioBufferProcessor(auto_start_recording=True)

    pipeline = Pipeline([
        # Incoming audio from call
        transport.input(),
        # OpeanAI Realtime listens, thinks, and generates speech
        llm,
        # Captures audio recording
        audiobuffer,
        # Outgoing audio back to call
        transport.output(),
    ])

    task = PipelineTask(pipeline, params=PipelineParams(allow_interruptions=True))

    runner = PipelineRunner()
    await runner.run(task)