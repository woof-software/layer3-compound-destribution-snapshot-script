const BN = require('bn.js');
const axios = require('axios');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;

const DAILY_ARB_REWARD = 1785;
const QUEST_START_TIME = 1719492300
const SUBGRAPH_API = '';
const SUBGRAPH_MARKET_ID = "";
const DUNE_API_KEY = '';

async function fetchSupplyMarketData(subgraphUrl, marketId, transactionThreshold, suppliers) {
    let lastSkip = 0;
    let allInteractions = [];
    const first = 1000

    while (true) {
        const query = `
        query {
            market(id: "${marketId}") {
                supplyBaseInteractions(where: {supplier_in: [${suppliers.map(supplier => `"${supplier}"`).join(', ')}],transaction_: {timestamp_gt: "${transactionThreshold}"}}, skip: ${lastSkip}, first: ${first}){
                    amount
                    amountUsd
                    supplier
                    transaction {
                        timestamp
                    }
                }
            }
        }`;

        const options = {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Accept': 'application/json',
            },
            body: JSON.stringify({ query })
        };

        try {
            const response = await fetch(subgraphUrl, options);
            const { data } = await response.json();
            const interactions = data.market.supplyBaseInteractions;

            if (!interactions.length) break;

            allInteractions.push(...interactions);
            lastSkip += first

            if (interactions.length < first) {
                break;  // Assumes there are no more data if less than `pageSize` results are returned.
            }
        } catch (error) {
            console.error('Error fetching data from The Graph:', error);
            break;
        }
    }

    return allInteractions;
}

async function fetchWithdrawMarketData(subgraphUrl, marketId, transactionThreshold, withdrawals) {
    let lastSkip = 0;
    let allInteractions = [];
    const first = 1000

    while (true) {
        const query = `
        query {
            market(id: "${marketId}") {
                    withdrawBaseInteractions(where: {transaction_:{timestamp_gt: "${transactionThreshold}" ,from_in: [${withdrawals.map(withdraw => `"${withdraw}"`).join(', ')}]}}, skip: ${lastSkip}, first: ${first}){
                    amount
                    transaction{
                      timestamp
                      from
                    }
                }
            }
        }`;

        const options = {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Accept': 'application/json',
            },
            body: JSON.stringify({ query })
        };

        try {
            const response = await fetch(subgraphUrl, options);
            const { data } = await response.json();
            const interactions = data.market.withdrawBaseInteractions;

            if (!interactions.length) break;

            allInteractions.push(...interactions);
            lastSkip += first

            if (interactions.length < first) {
                break;
            }
        } catch (error) {
            console.error('Error fetching data from The Graph:', error);
            break;
        }
    }

    return allInteractions;
}

const getLayer3QuestUsers = async () => {
    const response = await axios(`https://api.dune.com/api/v1/query/3868071/results`, { headers: { 'X-Dune-API-Key': DUNE_API_KEY, 'Content-Type': 'application/json' } })
    const addresses = response.data.result.rows
    const addressesInArray = addresses.map(({ address }) => address)
    const uniqueAddressArray = [...new Set(addressesInArray)];
    return uniqueAddressArray
}

const getSuppliers = async (users, timestamp) => {
    const allSuppliers = [];
    const maxAmountOfUsers = 1000;
    let i = 0;

    while (true) {
        const currentUsers = users.slice(i * maxAmountOfUsers, (i + 1) * maxAmountOfUsers)
        const arrayOfSupplies = await fetchSupplyMarketData(SUBGRAPH_API, SUBGRAPH_MARKET_ID, timestamp, currentUsers)
        allSuppliers.push(...arrayOfSupplies)
        if (currentUsers.length < 1000) break
        i += 1
    }

    return allSuppliers.map(({ amount, supplier, transaction: { timestamp } }) => ({ type: 'supply', amount, address: supplier, timestamp }))
}

const getWithdrawals = async (users, timestamp) => {
    const allWithdrawal = [];
    const maxAmountOfUsers = 1000;
    let i = 0;

    while (true) {
        const currentUsers = users.slice(i * maxAmountOfUsers, (i + 1) * maxAmountOfUsers)
        const arrayOfWithdrawal = await fetchWithdrawMarketData(SUBGRAPH_API, SUBGRAPH_MARKET_ID, timestamp, currentUsers)
        allWithdrawal.push(...arrayOfWithdrawal)
        if (currentUsers.length < 1000) break
        i += 1
    }

    return allWithdrawal.map(({ amount, transaction: { from, timestamp } }) => ({ type: 'withdraw', amount, address: from, timestamp }))
}

const getActionsByAddress = (supply, withdraw) => {
    const addresses = {};

    [...supply, ...withdraw].forEach((action) => {
        if (!addresses[action.address]) {
            addresses[action.address] = []
        }
        addresses[action.address].push(action)
    })

    return addresses
}

/**
 * Adds one hour to the given Unix timestamp.
 * 
 * @param {number} unixTimestamp - The Unix timestamp in seconds.
 * @returns {number} - The new Unix timestamp, with one hour added, in seconds.
 */
function addHours(unixTimestamp, hours = 1) {
    // One hour in seconds
    const oneHourInSeconds = hours * 3600;
    // Add one hour to the timestamp
    const newTimestamp = +unixTimestamp + oneHourInSeconds;
    return newTimestamp;
}

const calculateCurrentBalanceBasedOnPrevActions = (actions) => {
    let balance = new BN(0)

    actions.forEach(action => {
        if (action.type === 'supply') {
            balance = balance.add(new BN(action.amount))
        }
        if (action.type === 'withdraw') {
            balance = balance.sub(new BN(action.amount))
        }

        // in case when user withdraw funds that was in market before the campaign start
        if (balance.lt(new BN(0))) {
            balance = new BN(0)
        }
    })

    return balance.toString()
}

const getBalancesByHour = (allActions, startUnixTimestamp, finishUnixTimestamp) => {
    const uniquedAddresses = [...allActions.map((action) => action.address)]
    const usersActions = getActionsByAddress(allActions, [])

    let currentTime = startUnixTimestamp;
    const finishTime = finishUnixTimestamp;

    // [fromTimestamp-toTimestamp] -> address -> balance
    const actionsByHour = {}

    while (currentTime < finishTime) {
        const fromTimestamp = currentTime;
        const toTimestamp = addHours(currentTime)

        const key = `${fromTimestamp}-${toTimestamp}`
        actionsByHour[key] = {}

        uniquedAddresses.forEach((address) => {
            // { type, amount, address, timestamp }
            const userActions = usersActions[address];
            const userActionsBeforeCurrentTimestamp = userActions.filter(userAction => +userAction.timestamp < +toTimestamp);
            const userActionsBeforeCurrentTimestampSortedByTimestamp = userActionsBeforeCurrentTimestamp.sort((a, b) => +a.timestamp - +b.timestamp)
            const currentHourUserBalance = calculateCurrentBalanceBasedOnPrevActions(userActionsBeforeCurrentTimestampSortedByTimestamp)
            actionsByHour[key][address] = currentHourUserBalance
        })

        currentTime = addHours(currentTime)
    }

    return actionsByHour
}

const getTvlShanshotByHour = (balancesByHour) => {
    const hours = Object.keys(balancesByHour)
    const balanceByHour = {};

    hours.forEach(hour => {
        const tvl = Object.values(balancesByHour[hour]).reduce((accumulator, currentValue) => accumulator.add(new BN(currentValue)), new BN(0));
        balanceByHour[hour] = tvl.toString()
    })

    return balanceByHour
}

const getUserArbAmountPerHour = (balancesByHour, tvlByHour, arbPerHour) => {
    const hours = Object.keys(balancesByHour)
    const addressToHourToArbReward = {};

    hours.forEach(hour => {
        const tvl = tvlByHour[hour] // string number
        const userObj = balancesByHour[hour] // {address: "balance"}
        const addresses = Object.keys(userObj) // [address]
        addressToHourToArbReward[hour] = {}
        addresses.forEach(address => {
            const userBalance = userObj[address]
            const arbPerHourBN = new BN(arbPerHour).mul(new BN('1000000000000000000'));
            const a = new BN(userBalance).mul(new BN('1000000000000000000')); // mul on purpose
            const b = new BN(tvl)

            const result = a.div(b).mul(arbPerHourBN).div(new BN('1000000000000000000')) // div on purpose

            addressToHourToArbReward[hour][address] = result.toString()
        })
    })

    return addressToHourToArbReward
}

const getUserTotalArbReward = (timeToAddressToAmount) => {
    const addressToReward = {};

    const dates = Object.keys(timeToAddressToAmount)

    dates.forEach(date => {
        const rewardPerHourForUsers = timeToAddressToAmount[date]
        const addresses = Object.keys(rewardPerHourForUsers)
        addresses.forEach(address => {
            if (!addressToReward[address]) {
                addressToReward[address] = '0'
            }
            addressToReward[address] = new BN(addressToReward[address]).add(new BN(rewardPerHourForUsers[address])).toString()
        })
    })

    return addressToReward
}

const sumOfRewardsForAllUsers = (data) => {
    return (Object.values(data).reduce((accumulator, currentValue) => accumulator.add(new BN(currentValue)), new BN(0))).toString();
}

const prepareDataForCsv = (data) => {
    const result = []
    const addresses = Object.keys(data)
    addresses.forEach(address => {
        const numerator = new BN(data[address]);
        const denominator = new BN('1000000000000000000');
        const integerPart = numerator.div(denominator);
        const remainder = numerator.mod(denominator);
        const fractionalPart = remainder.toString(10).padStart(18, '0');
        const amount = `${integerPart.toString()}.${fractionalPart}`;
        result.push({ address, amountWei: data[address], amount })
    })

    return result
}

const main = async () => {
    const DAY_FROM_THE_START = 7
    const SNAPSHOT_FROM_TIMESTAMP = +QUEST_START_TIME;
    const SNAPSHOT_TO_TIMESTAMP = +addHours(QUEST_START_TIME, 24 * DAY_FROM_THE_START);

    if (SNAPSHOT_TO_TIMESTAMP > Math.floor(Date.now() / 1000)) {
        console.error('Cannot make snapshot. The final date is not reached')
        process.exit(0)
    }

    const users = await getLayer3QuestUsers();
    const suppliers = await getSuppliers(users, QUEST_START_TIME)
    const withdrawals = await getWithdrawals(users, QUEST_START_TIME)

    const allActions = [...suppliers, ...withdrawals]

    const getBalancesForUserPerHour = getBalancesByHour(allActions, +SNAPSHOT_FROM_TIMESTAMP, SNAPSHOT_TO_TIMESTAMP)
    const hourlyTvlSnapshot = getTvlShanshotByHour(getBalancesForUserPerHour)
    const usersArbRewardsByHours = getUserArbAmountPerHour(getBalancesForUserPerHour, hourlyTvlSnapshot, DAILY_ARB_REWARD / 24)
    const totalUsersRewards = getUserTotalArbReward(usersArbRewardsByHours)

    // verify how much arb should be destributed and how much will be destributed

    const destributedRewards = sumOfRewardsForAllUsers(totalUsersRewards)
    const shouldBeDestributed = new BN(DAY_FROM_THE_START * DAILY_ARB_REWARD).mul(new BN('1000000000000000000')).toString()
    const diff = new BN(destributedRewards).sub(new BN(shouldBeDestributed))

    if (diff.lt(new BN(0))) {
        console.log(`Will be destirbuted less arb then should for ${diff.toString()} in wei (devide it by 1e18)`)
    } else {
        console.log(`Will be destirbuted more arb then should for ${diff.toString()} in wei (devide it by 1e18)`)
    }

    // prepare and export to csv

    const dataForCsv = prepareDataForCsv(totalUsersRewards)
    const csvFilePath = `layer3-compound-ltipp-${SNAPSHOT_FROM_TIMESTAMP}-${SNAPSHOT_TO_TIMESTAMP}.csv`;
    const csvWriter = createCsvWriter({
        path: csvFilePath,
        header: [
            { id: 'address', title: 'Address' },
            { id: 'amountWei', title: 'ARB amount (wei)' },
            { id: 'amount', title: 'ARB amount' }
        ]
    })

    csvWriter.writeRecords(dataForCsv)
        .then(() => {
            console.log('CSV file was written successfully');
        })
        .catch((err) => {
            console.error('Error writing CSV file', err);
        });
}

main().then().catch()