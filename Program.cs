using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// For static file serving, no additional services are required.

var app = builder.Build();

// Configure the HTTP request pipeline.

if (app.Environment.IsDevelopment())
{
    app.UseDeveloperExceptionPage();
}

// Enable serving default files like index.html
app.UseDefaultFiles();

// Enable serving static files from wwwroot
app.UseStaticFiles();

// (Optional) If you have APIs or other middleware, configure them here
// app.MapControllers();

// If you have no other endpoints, ensure the app runs correctly
// app.Run();

app.Run();
