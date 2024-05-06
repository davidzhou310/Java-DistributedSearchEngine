//get parameter from URL
const href = window.location.search;
const urlParams = new URLSearchParams(href);
const keyword = JSON.parse(decodeURIComponent(urlParams.get('q')));

let page = 1, isLoading = false;

const searchForm = document.getElementsByName('search-form')[0];
const resultContainer = document.getElementsByName('result-container')[0];
const loadingContainer = document.getElementsByName('loading-container')[0];
const searchBar = document.getElementsByName('search-bar')[0];

searchForm.addEventListener('submit', (event) => {
    event.preventDefault();

    const searchBar = document.getElementsByName('search-bar')[0];
    const searchQuery = { keyword: searchBar.value.trim() };
    window.location.href = '/result?q=' + encodeURIComponent(JSON.stringify(searchQuery));
});

searchBar.placeholder = keyword.keyword;

function fetchResult() {
    if (isLoading) return;
    isLoading = true;
    loadingContainer.innerHTML = 'Loading...';

    fetch(`/search?query=${keyword.keyword}&page=${page}`)
        .then(res => {
            res.json().then(jsonRes => {
                if (jsonRes.length === 0) {
                    loadingContainer.innerHTML = 'No more results';
                    isLoading = false;
                    return;
                }
                jsonRes.forEach(element => {
                    resultContainer.innerHTML += `
                        <div class="search-result card mb-3 result-card mt-2">
                            <div class="card-body">
                                <a href="${element.url}" class="text-decoration-none text-dark">
                                    <h4 class="card-title">${element.url}</h3>
                                </a>
                            </div>
                        </div>
                    `;
                });
                loadingContainer.innerHTML = '';
                isLoading = false;
            }).catch(err => {
                isLoading = false;
                loadingContainer.innerHTML = '';
            });
        }).catch(err => {
            isLoading = false;
            loadingContainer.innerHTML = '';
        });
}

window.addEventListener('scroll', () => {
    if (isLoading) return;
    if (window.innerHeight + window.scrollY >= document.body.offsetHeight) {
        page++; 
        fetchResult();
    } 
});

resultContainer.innerHTML = '';
fetchResult();


