SHELL = /bin/sh
# .DEFAULT_GOAL=build

-include .env

init:
	conda create

x1:
	python mapd_ga_data.py "${GOOG_CREDS}" 'OmniSci Website_Live Site' "${OMNISCI_DB_URL}" 2019-11-01 2019-11-03

1month:
	python mapd_ga_data.py "${GOOG_CREDS}" 'OmniSci Website_Live Site' "${OMNISCI_DB_URL}" 30daysAgo today
