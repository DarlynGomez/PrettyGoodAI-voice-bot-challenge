import os

from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket
from fastapi.responses import PlainTextResponse
from twilio.rest import Client
from twilio.request_validator import RequestValidator
from dotenv import load_dotenv

from bot import run_bot

load_dotenv(override=True)

app = FastAPI()
# Authenticate Twilio's SDK client
twilio_client = Client(
    os.getenv("TWILIO_ACCOUNT_SID"),
    os.getenv("TWILIO_AUTH_TOKEN"),
)

# Current public ngrok URL
NGROK_URL = os.getenv("NGROK_URL")

@app.post("/start-call")
async def start_call():
    """
    Trigger endpoint that places the outbound call to the 
    PGAI test line.
    """

    twiml = f"""
    <Response>
        <Connect>
            <Stream url="wss://{NGROK_URL}/ws" />
        </Connect>
    </Response>
    """

    call = twilio_client.calls.create(
        # Test line
        to=os.getenv("TARGET_PHONE_NUMBER"),
        # Twilio purchased phone number
        from_=os.getenv("TWILIO_PHONE_NUMBER"),
        twiml=twiml,
    )

    return {
        "status": "calling", 
        "call_sid": call.sid
    }

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """
    Twillio connects to WebSocket once the call is answered and
    the TwiML instruction takes effect.
    """
    await websocket.accept()
    await run_bot(websocket)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)