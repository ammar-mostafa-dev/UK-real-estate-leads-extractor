from dataclasses import dataclass, asdict,fields

@dataclass
class Agency:
    # source_url is vital so we can verify the lead
    source_url:str 
    # Data We'll Be extracting
    name: str = "N/A"
    phone: str = "N/A"
    website_url: str = "N/A"
    address: str = "N/A"
    employees_range: str = "N/A"
    establish_date:str  = 'N/A' 
 
    @classmethod
    def get_headers(cls):
        """Loops Through the class attribute and returns them"""
        return [f.name for f in fields(cls)]

    def to_dict(self):
        """Converts the object to a dictionary for easy CSV export."""
        return asdict(self)

