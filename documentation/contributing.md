# Contributing

Below graph explains the process of contributing to Flow.  

```mermaid
flowchart TD
    subgraph "How to contribute"

        is-a-bug[Did you found a bug?]
        is-a-bug -->|Yes| can-you-fix-it[Can you fix it?]
        is-a-bug -->|NO| is-a-question[Do you have a question?]
        can-you-fix-it -->|NO| report[Please open a new **Bug Report**]
        can-you-fix-it -->|YES| bug-fix[Please open a pull request with with a fix]


        is-a-question -->|YES| go-to-discord[Ask a **Community Question**]
        is-a-question -->|NO| is-a-proposal[Do you want to propose new feature or a change?]

        is-a-proposal -->|NO| looking-for-tasks[Are you looking for tasks?]

        looking-for-tasks -->|YES| check-roadmap[Please check our Roadmap, there is always something to work on]

        is-a-proposal -->|YES| open-a-proposal[Open a **Proposal**]

        open-a-proposal --> wait-for-proposal-review["Give us time to review your proposal. We should get back to you in the first 24h (usually much faster)"]

        wait-for-proposal-review --> is-proposal-approved[Was your proposal approved by core contributors?]

        is-proposal-approved -->|YES| can-you-implement-it[Can you implement it?]
        is-proposal-approved -->|NO| proposal-rejected[Consider implementing that proposal through existing extension points or **Fork this Project**]

        can-you-implement-it -->|YES| submit-a-pr[Open a **Pull Request**]
        can-you-implement-it -->|No| can-you-sponsor[Can you sponsor that proposal?]

        can-you-sponsor -->|YES| sponsor-proposal[Awesome! Please mention that in the proposal description.]
        can-you-sponsor -->|NO| wait-for-others[No worries, you will need to wait for someone to implement this one for you.]
    end
```

## How to?

- [Submit a Bug Report](https://github.com/flow-php/flow/issues/new?template=bug)
- [Submit a Proposal](https://github.com/flow-php/flow/issues/new?template=proposal)
- Ask Community Question
  - [At Flow Discord Server](https://discord.gg/5dNXfQyACW) 
  - [At GitHub Discussions](https://github.com/flow-php/flow-php.com/discussions/categories/questions)
- [Sponsor Project of Proposal](http://flow-php.com/sponsor/)
- [Project Roadmap](https://github.com/orgs/flow-php/projects/1)

## Project Roadmap

> [!IMPORTANT]
> Flow PHP is managed through [GitHub Projects](https://github.com/orgs/flow-php/projects/1). 
> All code changes **MUST** first be added to the [Roadmap](https://github.com/orgs/flow-php/projects/1). 
> 
> Pull requests without an item in Roadmap might take significantly longer time to review or even get rejected due to missalignment
> with project architecture or future plans. In order to save time of contributors & maintainers, we would like to review
> the proposal before anyone starts to code. 
> 
> The only exception from that rule are Bug Fixes - those can be opened without opening a proposal first. 
> However it's still recommened to reach out as sometimes bugs might not be solvable imidiatelly or at all. 

If the idea you are planning to work on is not yet in the [Roadmap](https://github.com/orgs/flow-php/projects/1)
please create a new [Proposal Issue](https://github.com/flow-php/flow/issues/new?template=PROPOSAL) and wait for further guidance.

Proposal is considered "Accepted" when added to the Roadmap by one of the Core Contributors. 
By default proposal are added to the upcoming milestone, whatevr can't be delivered during milestone due date is going
to be moved to the next milestone.

> Weeks of coding can save you hours of planning. 

Opening a pull request without reaching out first might get rejected and eventually closed.   

**🐛 Bug Fixes** - bug fixes are the only exception from the above rule. Bug Fixes can be opened directly without a proposal issue.

## Before you start coding

Please make sure that you are aware of our [Architecture Decision Records](/documentation/adrs.md).
It's mandatory to follow all of them without any exceptions unless explicitly overridden by a new ADR.

## Next Steps

- [Setup development environment](/documentation/contributing/environment.md)
  - [Nix Shell](/documentation/contributing/nix.md)
- [Development Guidelines](/documentation/contributing/guidelines.md)
