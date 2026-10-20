"""Constants for Nord Pool."""

import logging

from homeassistant.const import Platform

LOGGER = logging.getLogger(__package__)

DOMAIN = "nordpool"
PLATFORMS = [Platform.BINARY_SENSOR, Platform.SENSOR]
DEFAULT_NAME = "Nord Pool"

CONF_AREAS = "areas"
ATTR_RESOLUTION = "resolution"
