namespace StreamLargeFile;

public record Person
{
    public long Id { get; set; } = default!;             
    public string FirstName { get; set; } = default!;      
    public string LastName { get; set; } = default!;       
    public int Age { get; set; } = default!;               
    public string Email { get; set; } = default!;          
    public string Address { get; set; } = default!;        
    public string PhoneNumber { get; set; } = default!;    
    public DateTime CreatedAt { get; set; } = default!;    
}