using DocCollabMongoApi.Hubs;
using DocCollabMongoCore.Domain.DocumentCollab;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.SignalR;
using Syncfusion.EJ2.DocumentEditor;

namespace DocCollabMongoApi.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class DocumentCollabController : ControllerBase
    {
        private readonly IHubContext<DocumentEditorHub> _hubContext;
        private readonly ILogger<DocumentCollabController> _logger;

        public DocumentCollabController(IHubContext<DocumentEditorHub> hubContext, ILogger<DocumentCollabController> logger)
        {
            _hubContext = hubContext;
            _logger = logger;
        }

        [HttpPost]
        [Route("ImportFile")]
        public Task<string> ImportFile([FromServices] DocumentCollabWriteHandler handler, FileCollabDetails fileInfo) => handler.ImportFileAsync(fileInfo);

        [HttpPost("updateAction")]
        public async Task<ActionInfo?> UpdateAction([FromServices] DocumentCollabWriteHandler handler, [FromBody] ActionInfo param)
        {
            _logger.LogInformation($"ActionInfo for RoomName: {param.RoomName}, for User: {param.CurrentUser} - {param}");
            ActionInfo modifiedAction = handler.UpdateActionAsync(param);
            await _hubContext.Clients.Group(param.RoomName).SendAsync("dataReceived", "action", modifiedAction);
            return modifiedAction;
        }

        [HttpPost("getActionsFromServer")]
        public Task<string> GetActionsFromServer([FromServices] DocumentCollabWriteHandler handler, [FromBody] ActionInfo param) => handler.GetActionsFromServerAsync(param);
    }
}
