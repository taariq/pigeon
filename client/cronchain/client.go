package cronchain

import (
	"context"

	codectypes "github.com/cosmos/cosmos-sdk/codec/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/gogo/protobuf/grpc"
	"github.com/vizualni/whoops"

	"github.com/cosmos/cosmos-sdk/crypto/keyring"
	chain "github.com/volumefi/conductor/client"
	concensustypes "github.com/volumefi/conductor/types/volumefi/cronchain/x/consensus/types"
)

type Client struct {
	L *chain.LensClient
}

type QueuedMessage[T concensustypes.Signable] struct {
	ID    uint64
	Nonce []byte
	Msg   T
}

// QueryMessagesForSigning returns a list of messages from a given queueTypeName that
// need to be signed by the provided validator given the valAddress.
func QueryMessagesForSigning[T concensustypes.Signable](
	ctx context.Context,
	c Client,
	valAddress string,
	queueTypeName string,
) ([]QueuedMessage[T], error) {
	return queryMessagesForSigning[T](ctx, c.L, c.L.Codec.Marshaler, valAddress, queueTypeName)
}

func queryMessagesForSigning[T concensustypes.Signable](
	ctx context.Context,
	c grpc.ClientConn,
	anyunpacker codectypes.AnyUnpacker,
	valAddress string,
	queueTypeName string,
) ([]QueuedMessage[T], error) {
	qc := concensustypes.NewQueryClient(c)
	msgs, err := qc.QueuedMessagesForSigning(ctx, &concensustypes.QueryQueuedMessagesForSigningRequest{
		ValAddress:    valAddress,
		QueueTypeName: queueTypeName,
	})
	if err != nil {
		return nil, err
	}
	var res []QueuedMessage[T]
	for _, msg := range msgs.GetMessageToSign() {
		var m concensustypes.Signable
		err := anyunpacker.UnpackAny(msg.GetMsg(), &m)
		if err != nil {
			return nil, whoops.Wrap(err, ErrUnableToUnpackAny)
		}
		msgT, ok := m.(T)
		if !ok {
			var expectedType T
			return nil, ErrIncorrectTypeSavedInMessage.Format(expectedType, m)
		}
		res = append(res, QueuedMessage[T]{
			ID:    msg.GetId(),
			Nonce: msg.GetNonce(),
			Msg:   msgT,
		})
	}

	return res, nil
}

type BroadcastMessageSignatureIn struct {
	ID            uint64
	QueueTypeName string
	Signature     []byte
}

// BroadcastMessageSignatures takes a list of signatures that need to be sent over to the chain.
// It build the message and sends it over.
func (c Client) BroadcastMessageSignatures(ctx context.Context, signatures ...BroadcastMessageSignatureIn) error {
	return broadcastMessageSignatures(ctx, c.L, signatures...)
}

type msgSender interface {
	SendMsg(ctx context.Context, msg sdk.Msg) (*sdk.TxResponse, error)
}

func broadcastMessageSignatures(
	ctx context.Context,
	ms msgSender,
	signatures ...BroadcastMessageSignatureIn,
) error {
	if len(signatures) == 0 {
		return nil
	}
	var signedMessages []*concensustypes.MsgAddMessagesSignatures_MsgSignedMessage
	for _, sig := range signatures {
		signedMessages = append(signedMessages, &concensustypes.MsgAddMessagesSignatures_MsgSignedMessage{
			Id:            sig.ID,
			QueueTypeName: sig.QueueTypeName,
			Signature:     sig.Signature,
		})
	}
	msg := &concensustypes.MsgAddMessagesSignatures{
		SignedMessages: signedMessages,
	}
	_, err := ms.SendMsg(ctx, msg)
	return err
}

func (c Client) Keyring() keyring.Keyring {
	return c.L.Keybase
}
