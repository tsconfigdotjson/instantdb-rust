Build a full instant sync engine implementation in rust, do what this team never did and make it stateless and adapter driven (more backends to follow) to it just plugs into a standard Postgres database, the sync engine should scale horizontally statelessly, tenanted if needed, supporting google oauth and the full permissions  stack instant implemented, it should just snap in and work with existing clients, we aim to be a  service the people can migrate to easily

this team is sunsetting instant so this is heroic and users will love it.
the full legacy codebase is in LEGACY/
we want full unit testing, full scenario testing to match the level of testing they had done here

You should be able to use your local environment to test here, run tests, create docker instances, anything you need for a full validation loop without a human.

Dont stop until its totally totally done and we have 100% parity with instant, You can include a react todo list example with their existing react client working with this implementation, use your chrome plugin for full validation there if needed.
