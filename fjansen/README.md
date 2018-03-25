# Project

## Description
This project merges the (unrelated) NYC 311 and Boston Fire Incident data sets, grouping them by date with a count per type.

## Auth setup
Use the following format for the `auth.json` file:

```json
{
  "services": {
    "nycportal": {
      "service": "https://data.cityofnewyork.us/",
      "username": "xxx",
      "token": "xxx",
      "key": "xxx"
    }
  }
}
```

The token and key can be requested at the [NYC Data Portal](https://data.cityofnewyork.us/profile/).
