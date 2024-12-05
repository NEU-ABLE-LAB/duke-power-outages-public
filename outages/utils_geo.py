"""
Types and helpers for geospatial data.
"""

import hashlib

class BoundingBox:

    """
    A class to represent a bounding box with southwest and northeast coordinates.
    
    Attributes:
        sw_lat (float): Southwest latitude.
        sw_long (float): Southwest longitude.
        ne_lat (float): Northeast latitude.
        ne_long (float): Northeast longitude.
    """
    
    def __init__(self,
            sw_lat: float,
            sw_long: float,
            ne_lat: float,
            ne_long: float,
            name: str = None,
            jurisdiction: str = None,
            dashboard_index: int = None,
        ):
        """
        Constructs all the necessary attributes for the bounding box object.
        
        Args:
            sw_lat (float): Southwest latitude.
            sw_long (float): Southwest longitude.
            ne_lat (float): Northeast latitude.
            ne_long (float): Northeast longitude.
            name (str, optional): Name of the bounding box. 
                If not provided, it will be computed based on the coordinates.
            jurisdiction (str, optional): Duke Energy jurisdiction for the bounding box.
                If not provided, it will be determined based on the coordinates.
            dashboard_index (int, optional): Index of the bounding box in the dashboard.
        """
        self._sw_lat = sw_lat
        self._sw_long = sw_long
        self._ne_lat = ne_lat
        self._ne_long = ne_long
        self._name = name if name is not None else self._compute_hash()
        self._jurisdiction = jurisdiction if jurisdiction is not None else self.jurisdiction
        self._dashboard_index = dashboard_index

    @property
    def sw_lat(self):
        """Gets the southwest latitude."""
        return self._sw_lat

    @property
    def sw_long(self):
        """Gets the southwest longitude."""
        return self._sw_long

    @property
    def ne_lat(self):
        """Gets the northeast latitude."""
        return self._ne_lat

    @property
    def ne_long(self):
        """Gets the northeast longitude."""
        return self._ne_long

    @property
    def bounds(self):
        """
        Gets the bounding box as a tuple of coordinates.
        
        Returns:
            tuple: A tuple containing (sw_lat, sw_long, ne_lat, ne_long).
        """
        return (self.sw_lat, self.sw_long, self.ne_lat, self.ne_long)

    def _compute_hash(self):
        """
        Computes a hash for the bounding box based on its coordinates.
        
        Returns:
            str: A string representing the hash of the bounding box.
        """
        hash_input = f"{self.sw_lat},{self.sw_long},{self.ne_lat},{self.ne_long}".encode()
        return hashlib.md5(hash_input).hexdigest()

    @property
    def name(self):
        """Gets the name of the bounding box."""
        return self._name
    
    @property
    def jurisdiction(self):
        """
        Determines the jurisdiction based on whether part of the bounding box lies 
        within DUKE_CAROLINAS_REGION or DUKE_FLORIDA_REGION.
        
        Returns:
            str: 'DEC' if part of the BoundingBox lies within DUKE_CAROLINAS_REGION,
                 'DEF' if part of the BoundingBox lies within DUKE_FLORIDA_REGION,
                 or None if it lies outside both regions.
        """

        if not(hasattr(self, '_jurisdiction')) or self._jurisdiction is None:
            if self._intersects(DUKE_CAROLINAS_REGION):
                return 'DEC'
            elif self._intersects(DUKE_FLORIDA_REGION):
                return 'DEF'
            else:
                raise ValueError("BoundingBox lies outside both DUKE_CAROLINAS_REGION and DUKE_FLORIDA_REGION")
        else:
            return self._jurisdiction

    def _intersects(self, other):
        """
        Checks if this bounding box intersects with another bounding box.

        Args:
            other (BoundingBox): Another bounding box to check intersection with.

        Returns:
            bool: True if there is an intersection, False otherwise.
        """
        return not (self.ne_lat < other.sw_lat or  # self is below other
                    self.sw_lat > other.ne_lat or  # self is above other
                    self.ne_long < other.sw_long or  # self is left of other
                    self.sw_long > other.ne_long)  # self is right of other
    
    @property
    def dashboard_index(self) -> int:
        """
        Gets the index of the bounding box in the dashboard.

        Returns:
            int: The index of the bounding box in the dashboard.
        """
        return self._dashboard_index

DUKE_CAROLINAS_REGION = BoundingBox(
    33.1, -84.4, 36.6, -76.0,
    name='duke_carolinas',
    jurisdiction='DEC',
    dashboard_index=0,
)
DUKE_FLORIDA_REGION = BoundingBox(
    26.9, -86.0, 30.6, -80.5,
    name='duke_florida',
    jurisdiction='DEF',
    dashboard_index=1,
)
DUKE_INDIANA_REGION = BoundingBox(
    37.7, -88.1, 41.5, -84.7,
    name='duke_indiana',
    jurisdiction='DEI',
    dashboard_index=2,
)
# -88.09789189	37.77174303
# -84.78457998	41.43690098

DUKE_OHIO_KENTUCKY_REGION = BoundingBox(
    38.3, -85.1, 40.0, -83.2,
    name='duke_ohio_kentucky',
    jurisdiction='DEM',
    dashboard_index=3,
)
# -85.07603692	38.338596
# -83.26769501	39.92297595


REGIONS = [DUKE_CAROLINAS_REGION, DUKE_FLORIDA_REGION,
           DUKE_INDIANA_REGION, DUKE_OHIO_KENTUCKY_REGION]
