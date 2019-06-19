#!/bin/sh
# figure out if this actually POSIX shell and not Bash

### CONFIGURATION

set -ex

### TEMPLATE
FAUCET_SK="ed25519e_sk1hq34w0kxkjncm879m5fty49pnkpeuv0ryrv9lpy3a0e66jqd7pv70hvgmteqz44lr42ukemcu9u9jtc46gtq3z0wuy4yher7gfe493qgc8nph"
BLOCK0_HASH="3ee47fa622b1cf6e5aea5c27a773d3f6fb030c59b27b06ad4a224b76f48776bd"
REST_URL="http://127.0.0.1:8443/api"
CLI="jcli"
COLORS=1
FEE_CONSTANT=10
FEE_CERTIFICATE=0
FEE_COEFFICIENT=0
ADDRTYPE="--testing"
STAKE_POOL_ID="48b5ad9e256efbc506f0fe7611638495fdab2e51a39af31a1214074352b0659c"

### COLORS
if [ ${COLORS} -eq 1 ]; then
    GREEN=`printf "\033[0;32m"`
    RED=`printf "\033[0;31m"`
    BLUE=`printf "\033[0;33m"`
    WHITE=`printf "\033[0m"`
else
    GREEN=""
    RED=""
    BLUE=""
    WHITE=""
fi

if [ ${#} -ne 0 ]; then
    exit 1
fi

##
# 1. create an account
##

# create the account secret, public and address
ACCOUNT_SK=$($CLI key generate --type=ed25519extended)
ACCOUNT_SK_FILE="account.prv"
echo ${ACCOUNT_SK} > ${ACCOUNT_SK_FILE}
ACCOUNT_PK=$(echo ${ACCOUNT_SK} | $CLI key to-public)
ACCOUNT_ADDR=$(echo ${ACCOUNT_PK} | xargs $CLI address account ${ADDRTYPE})
ACCOUNT_COUNTER=0

# send money to this address
./faucet-send-money.sh ${ACCOUNT_ADDR} 1000

sleep 12

##
# 2. create a new certificate to delegate our new address's stake
#    to a stake pool
##

echo "creating certificate"

CERTIFICATE_FILE="account_delegation_certificate"
SIGNED_CERTIFICATE_FILE="account_delegation_certificate.signed"

$CLI certificate new stake-delegation \
    ${STAKE_POOL_ID} \
    ${ACCOUNT_PK} \
    ${CERTIFICATE_FILE}
$CLI certificate sign \
    ${ACCOUNT_SK_FILE} \
    ${CERTIFICATE_FILE} \
    ${SIGNED_CERTIFICATE_FILE}

##
# 3. now create a transaction and sign it
##

# we know the account has no transaction in the past, so the counter is 0
# and has remained 0

TRANSACTION_FILE=transaction
SIGNED_TRANSACTION_FILE=transaction.signed
WITNESS_FILE=transaction.witness

# the fee to post the new certificate, the coefficient is just
# multiplied by one because we only have one input
POST_CERTIFICATE_FEE=$((${FEE_CONSTANT} + ${FEE_CERTIFICATE} + ${FEE_COEFFICIENT}))

$CLI transaction new --staging=${TRANSACTION_FILE}
$CLI transaction add-account --staging=${TRANSACTION_FILE} ${ACCOUNT_ADDR} ${POST_CERTIFICATE_FEE}
cat ${SIGNED_CERTIFICATE_FILE} | xargs $CLI transaction add-certificate --staging=${TRANSACTION_FILE}
$CLI transaction finalize --staging=${TRANSACTION_FILE}

# get the transaction id
TRANSACTION_ID=$($CLI transaction id --staging=${TRANSACTION_FILE})

# create the witness
$CLI transaction make-witness ${TRANSACTION_ID} \
    --genesis-block-hash ${BLOCK0_HASH} \
    --type "account" --account-spending-counter "${ACCOUNT_COUNTER}" \
    ${WITNESS_FILE} ${ACCOUNT_SK_FILE}
$CLI transaction add-witness --staging=${TRANSACTION_FILE} ${WITNESS_FILE}

$CLI transaction seal --staging=${TRANSACTION_FILE}

$CLI transaction to-message --staging ${TRANSACTION_FILE} | $CLI rest v0 message post -h "${REST_URL}"

echo "Now check the proper setting at posted:"
echo "jcli rest v0 account get ${ACCOUNT_PK} -h \"${REST_URL}\""
exit 0