#!/bin/bash --
cd /home/jfish/project_data/boring_cities/data/
sqlite3 <<EOF
.read /home/jfish/evictionlab-projects/boring_cities/business_locations_flat.sql
.read /home/jfish/evictionlab-projects/boring_cities/business_locations_panel.sql
.exit
EOF