# QUOTAMONITOR

Quotamonitor is a script that is used to check quotas across multiple different storage systems, notify owners of, and write quota information to CSV files and a mysql database. At this time it supports the following storage system vendors: 
* DellEMC Isilon
* Qumulo
* Vast Data
* Nexenta
* Racktop

and can be incorporated with Starfish in order to implement soft qutoas on a storage system that does not have inbuilt hard quotas. 

The script also supports checking the quota size of a mount via df. 

For the purposes of this script, quotas are assumed to be set on directories and are hard. At this time, the script does not support user or group quotas, or soft or advisory quotas. It could be modified to support these functions if desired. 

Quotamonitor has been tested on RHEL/CentOS/Scientific Linux 7.x and assumes a Linux operating system, although hopefully I've coded it in a way that it could be modified fairly easily to support Windows. 

Quotamonitor requires Python 2.7, as Qumulo does not yet support Python 3.x. 

Please see the top of the script for the Python modules required. 

## Configuration
Configuration is stored in a json file in the same directory as quotamonitor.py. An example configuration is provided, although it may not include all of the possible permutations of the configuration. 

The "email settings" stanza of the configuration json file is email settings, which is set up for basic smtp relay (no ssl or auth). It includes the smtp server, a sender address, and a default recipient to whom all emails will be sent. It also has a default alerting percent (for the warn threshold), a path to the directory containing the template, and default subject lines. 

The "db_settings" stanza is for a database connection, if you have need for that (if not, comment out the line in the main section of the script which writes to the db.) The mapping allows you to map multiple storage systems to a tier so that all items in that tier are stored in the same table in the database. 

The "application_shares" stanza exists so that a specific email and header can be sent out for shares that don't fit the group-share model, for example if there is an application that has its own shares/exports carved out of one of the other storage systems. In this case, the application owner may want to have a different notification emails and different thresholds set. 

In "storagesystems", connection information to the various storage systems is defined, including url, username, password, and type, where type corresponds to the various class definitions in the script, currently including Isilon, Qumulo, Nexenta, Racktop, and Vast Data. The "nfsmapping" dictionary allows the administrator to map the mounted location of the export to the storage system native location of said export. Note that for the Nexenta entries, there is also a toplevel entry that corresponds to the pool name. The logfile entry is for the path to the csv file where the quotas are logged. 
"Groups" is where individual quota owners, email addresses, and warn percentages can be defined. Note that if a quota is not listed in this section, the default settings set in the "email_settings" stanza will apply. If "application_shares" exist, those must be called here in the "application" key. 

## License
This project is licensed under the terms of the MIT license.
