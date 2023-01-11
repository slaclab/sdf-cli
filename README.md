# SDF-CLI

This repo contains command line tools for the SDF to provide a single resource from which all user and administrative tools can be accessed from.

# Features

- TBD



# Development

This is based upon the [cliff](https://docs.openstack.org/cliff/latest/index.html) command line frameworkwhich provides a clean separation of Command classes from which we can create a hierarchy of commands ala git etc. so that we may provide a logical noun-verb syntax to our utilities.

We create a high level abstration for the cliff App class such that provide one more level of command in this command tree. This is implemented as a MultiApp class that should be instantiated with a List of command_managers - their `__name__` should be unique.





# CoactD

to provide the microservice abstration of user and disk requests from coact, we provide a daemon in sdf-cli to enact the required workflows for new user and repo registrations.

to run, do

    ❯ SDF_COACT_URI=wss://coact-dev.slac.stanford.edu/graphql-service  ./sdf.py coactd get

note that the uri scheme is wss.
