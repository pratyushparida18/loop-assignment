from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
import uvicorn
from app.routers.app_routers import router as app_router



app = FastAPI()

# Set up CORS
app.add_middleware( 
    CORSMiddleware,
    allow_origins=["https://pdf-management-and-collaboration-app.vercel.app"],  # Replace with your React app URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Include the routers
app.include_router(app_router,tags=["app routes"])

# Start the application
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, debug=True)
