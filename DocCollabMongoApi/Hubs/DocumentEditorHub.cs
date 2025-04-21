using DocCollabMongoCore;
using DocCollabMongoCore.Domain.DocumentCollab;
using Microsoft.AspNetCore.SignalR;
using Syncfusion.EJ2.DocumentEditor;

namespace DocCollabMongoApi.Hubs;


public sealed class DocumentEditorHub : Hub
{
    private static readonly Dictionary<string, ActionInfo> s_userManager = [];
    internal static readonly Dictionary<string, List<ActionInfo>> GroupManager = [];
    private readonly DocumentCollabWriteHandler _documentCollabWriteHandler;

    public DocumentEditorHub(DocumentCollabWriteHandler documentCollabWriteHandler) => _documentCollabWriteHandler = documentCollabWriteHandler;

    public async Task JoinGroup(ActionInfo info)
    {
        s_userManager.TryAdd(Context.ConnectionId, info);

        info.ConnectionId = Context.ConnectionId;
        //Add to SignalR group
        await Groups.AddToGroupAsync(Context.ConnectionId, info.RoomName);

        if (GroupManager.TryGetValue(info.RoomName, out var value))
        {
            await Clients.Caller.SendAsync("dataReceived", "addUser", value);
        }

        lock (GroupManager)
        {
            if (GroupManager.TryGetValue(info.RoomName, out var group))
            {
                group.Add(info);
            }
            else
            {
                var actions = new List<ActionInfo> { info };
                GroupManager.Add(info.RoomName, actions);
            }
        }
        //Send information about new user joining to others
        await Clients.GroupExcept(info.RoomName, Context.ConnectionId).SendAsync("dataReceived", "addUser", info);

        //s_userManager.TryAdd(Context.ConnectionId, info);

        //info.ConnectionId = Context.ConnectionId;
        ////Add to SignalR group
        //await Groups.AddToGroupAsync(Context.ConnectionId, info.RoomName);

        //lock (GroupManager)
        //{
        //    if (GroupManager.TryGetValue(info.RoomName, out var group))
        //    {
        //        // Check if user already exists in the group based on CreatedUser property
        //        var existingUser = group.FirstOrDefault(u => u.CurrentUser == info.CurrentUser);
        //        if (existingUser is { })
        //        {
        //            // Remove the existing user before adding again
        //            group.Remove(existingUser);

        //            Clients.OthersInGroup(info.RoomName).SendAsync("dataReceived", "removeUser", existingUser.ConnectionId);
        //            Groups.RemoveFromGroupAsync(existingUser.ConnectionId, info.RoomName);
        //            s_userManager.Remove(existingUser.ConnectionId);
        //        }

        //        group.Add(info);
        //    }
        //    else
        //    {
        //        var actions = new List<ActionInfo> { info };
        //        GroupManager.Add(info.RoomName, actions);
        //    }
        //}
        //// Send existing users to the caller
        //if (GroupManager.TryGetValue(info.RoomName, out var value))
        //{
        //    await Clients.Caller.SendAsync("dataReceived", "addUser", value);
        //}

        ////Send information about new user joining to others
        //await Clients.GroupExcept(info.RoomName, Context.ConnectionId).SendAsync("dataReceived", "addUser", info);
    }

    public override Task OnConnectedAsync()
    {
        //Send connection id to client side
        _ = Clients.Caller.SendAsync("dataReceived", "connectionId", Context.ConnectionId);
        return base.OnConnectedAsync();
    }

    public override async Task OnDisconnectedAsync(Exception? exception)
    {
        if (s_userManager.Count != 0 && s_userManager.TryGetValue(Context.ConnectionId, out var userData))
        {
            var roomName = userData.RoomName;
            if (GroupManager.TryGetValue(roomName, out var groupMembers))
            {
                groupMembers.Remove(userData);

                if (groupMembers.Count == 0)
                {
                    var collectionName = $"{ApplicationConstant.DocumentCollabTempTablePrefix}{roomName}";
                    GroupManager.Remove(roomName);
                    //Push all the updates to the master collection and Publish the same
                    _documentCollabWriteHandler.UpdateOperationsToMasterTableAsync(roomName, collectionName, false, 0);

                    //Drop the temporary collection on disconnection
                    _documentCollabWriteHandler.DropTemporaryCollection(collectionName);
                }
            }

            //Send notification about user disconnection to other clients.
            await Clients.OthersInGroup(roomName).SendAsync("dataReceived", "removeUser", Context.ConnectionId);
            await Groups.RemoveFromGroupAsync(Context.ConnectionId, roomName);
            s_userManager.Remove(Context.ConnectionId);
        }

        await base.OnDisconnectedAsync(exception);
    }
}

