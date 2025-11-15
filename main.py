from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import date
from database import db, create_document, get_documents

app = FastAPI(title="ORchestrator.ai Scheduling Engine MVP")

# --- Schemas (for API validation only; database schemas live in schemas.py) ---
class Provider(BaseModel):
    id: str
    name: str
    call_sign: Optional[str] = None
    fte: float = Field(ge=0, le=1)
    acc_target: int
    call_target: int
    site_preferences: List[str] = []
    qualifications: List[str] = []
    seniority_level: int = 0
    politics_weight: float = 0.0

class ShiftType(BaseModel):
    id: str
    name: str  # REG/APS/CALL/OFF
    site: str
    weekly: bool = False
    requires_qualification: Optional[str] = None

class Assignment(BaseModel):
    provider_id: str
    date: date
    shift_type: str
    site: str
    generated_by: str = "AI"  # or Human
    audited_by: Optional[str] = None

class Quarter(BaseModel):
    year: int
    quarter: int
    acc_balance: int = 0
    call_balance: int = 0

# Collections
PROVIDER_COL = "provider"
SHIFT_COL = "shifttype"
ASSIGN_COL = "assignment"

# --- API Endpoints ---
@app.get("/")
def root():
    return {"message": "ORchestrator.ai backend running"}

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/test")
def test_db():
    status = {
        "backend": "ok",
        "database": "connected" if db is not None else "unavailable",
        "database_url": "set" if db is not None else "unset",
        "database_name": getattr(db, "name", None),
        "connection_status": "ok" if db is not None else "error",
        "collections": []
    }
    try:
        if db is not None:
            status["collections"] = sorted(await_list_collections())
    except Exception as e:
        status["connection_status"] = f"error: {e}"
    return status


def await_list_collections():
    try:
        return db.list_collection_names() if db is not None else []
    except Exception:
        return []

@app.post("/providers", response_model=Provider)
def create_provider(provider: Provider):
    # Upsert into DB keyed by id
    if db is None:
        raise HTTPException(status_code=500, detail="Database not configured")
    # Ensure id uniqueness
    existing = db[PROVIDER_COL].find_one({"id": provider.id})
    if existing:
        raise HTTPException(status_code=400, detail="Provider already exists")
    create_document(PROVIDER_COL, provider)
    return provider

@app.get("/providers", response_model=List[Provider])
def list_providers():
    if db is None:
        raise HTTPException(status_code=500, detail="Database not configured")
    docs = get_documents(PROVIDER_COL)
    return [Provider(**{k: v for k, v in d.items() if k in Provider.model_fields}) for d in docs]

@app.post("/shift-types", response_model=ShiftType)
def create_shift_type(shift: ShiftType):
    if db is None:
        raise HTTPException(status_code=500, detail="Database not configured")
    existing = db[SHIFT_COL].find_one({"id": shift.id})
    if existing:
        raise HTTPException(status_code=400, detail="ShiftType already exists")
    create_document(SHIFT_COL, shift)
    return shift

@app.get("/shift-types", response_model=List[ShiftType])
def list_shift_types():
    if db is None:
        raise HTTPException(status_code=500, detail="Database not configured")
    docs = get_documents(SHIFT_COL)
    return [ShiftType(**{k: v for k, v in d.items() if k in ShiftType.model_fields}) for d in docs]

class GenerateRequest(BaseModel):
    start_date: date
    end_date: date

class GenerateResponse(BaseModel):
    created: int
    conflicts: List[str]

@app.post("/generate", response_model=GenerateResponse)
def generate_schedule(req: GenerateRequest):
    if db is None:
        raise HTTPException(status_code=500, detail="Database not configured")
    providers = list(db[PROVIDER_COL].find())
    shifts = list(db[SHIFT_COL].find({"name": "REG"}))
    if not providers or not shifts:
        raise HTTPException(status_code=400, detail="Providers and REG Shift Type required")

    conflicts: List[str] = []
    days = (req.end_date - req.start_date).days + 1
    created = 0
    p = 0
    site = shifts[0]["site"]
    needed_qual = shifts[0].get("requires_qualification")

    for i in range(days):
        day = req.start_date.toordinal() + i
        day_date = date.fromordinal(day)
        for _ in range(min(len(providers), len(shifts))):
            tries = 0
            while tries < len(providers):
                prov = providers[p % len(providers)]
                p += 1
                tries += 1
                # Prevent double booking same day
                if db[ASSIGN_COL].find_one({"provider_id": prov["id"], "date": str(day_date)}):
                    continue
                # Qualification gate
                if needed_qual and needed_qual not in (prov.get("qualifications") or []):
                    continue
                # Assign
                create_document(ASSIGN_COL, {
                    "provider_id": prov["id"],
                    "date": str(day_date),
                    "shift_type": "REG",
                    "site": site,
                    "generated_by": "AI"
                })
                created += 1
                break
            else:
                conflicts.append(f"No eligible provider for {day_date}")
    return GenerateResponse(created=created, conflicts=conflicts)

@app.get("/assignments", response_model=List[Assignment])
def list_assignments():
    if db is None:
        raise HTTPException(status_code=500, detail="Database not configured")
    docs = list(db[ASSIGN_COL].find())
    # dates stored as string; convert for response
    cleaned = []
    for d in docs:
        payload = {
            "provider_id": d.get("provider_id"),
            "date": date.fromisoformat(d.get("date")),
            "shift_type": d.get("shift_type"),
            "site": d.get("site"),
            "generated_by": d.get("generated_by", "AI"),
            "audited_by": d.get("audited_by")
        }
        cleaned.append(Assignment(**payload))
    return cleaned
