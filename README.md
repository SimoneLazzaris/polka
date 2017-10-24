# polka
Simple but effetctive policy daemon for postfix.

This simple daemon counts the number of messages sent by authenticated users OR by IP (if the SMTP AUTH is not used),
storing the information in a simple mysql table.

By default, a new entry is created with a default limit (read by configuration), but that value can be overriden
by the sysadmin simpli updating the table.

In addition, the daemon refuses messages from/to blacklisted sender/recipient, also stored in mysql tables.

Used in production and very stable and effective.
